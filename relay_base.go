package eventstore

import (
	"context"
	"hash/fnv"
	"sync"
	"time"
)

// relayConfig carries the knobs collected from RelayOption. It is shared
// by every concrete relay type as an embedded field so the option
// setters can write through a single struct. Each relay also holds a
// single typed handler factory and a concrete strategy that closes over
// that factory.
type relayConfig struct {
	batchSize             int
	batchDelay            time.Duration
	conditionalBatchDelay time.Duration
	parallelism           int
}

// pickWorker hashes the event's EntityID to a stable worker index. The same
// EntityID always lands on the same worker for a given n, so per-aggregate
// ordering is preserved across batches.
func pickWorker(ev StoredEvent, n int) int {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	id := ev.EntityID
	_, _ = h.Write(id[:])
	return int(h.Sum32() % uint32(n))
}

// recordFirstErr stores err in *dst if *dst is still nil, guarded by mu.
// Used by the parallel worker pool to collect the first handler error
// across workers.
func recordFirstErr(mu *sync.Mutex, dst *error, err error) {
	mu.Lock()
	defer mu.Unlock()
	if *dst == nil {
		*dst = err
	}
}

// buildPlainHandler invokes the Handler factory for the given worker.
// Returns nil when no factory is configured. Used by handlerBatchStrategy
// for both the sequential path and per-worker factory invocation.
func buildPlainHandler(factory func(WorkerContext) Handler, wc WorkerContext) Handler {
	if factory == nil {
		return nil
	}
	return factory(wc)
}

// buildBatchHandler invokes the BatchHandler factory for the given worker.
// Returns nil when no factory is configured. Used by
// batchHandlerBatchStrategy.
func buildBatchHandler(factory func(WorkerContext) BatchHandler, wc WorkerContext) BatchHandler {
	if factory == nil {
		return nil
	}
	return factory(wc)
}

// parallelWorkerArgs bundles the inputs that a strategy-specific worker
// goroutine needs. The factory is typed (`Handler` or `BatchHandler`) —
// the concrete strategy closes over the right one.
type parallelWorkerArgs struct {
	ctx      context.Context
	ch       <-chan StoredEvent
	wc       WorkerContext
	errMu    *sync.Mutex
	firstErr *error
	cancel   context.CancelFunc
}

// runParallel orchestrates the worker pool: spawn one goroutine per
// worker, dispatch each event by hash(EntityID), and wait for all workers
// to drain. The startWorker closure is supplied by the concrete relay
// type — it knows how to invoke the registered factories and the
// per-event logic. The waitGroup is also created here so callers can
// wg.Wait() after startWorker has been launched.
//
// ctx is wrapped in a child context so the pool can broadcast cancellation
// to all workers when the first one errors.
func runParallel(
	ctx context.Context,
	batch []StoredEvent,
	n int,
	startWorker func(a parallelWorkerArgs, wg *sync.WaitGroup),
) (int64, []StoredEvent, error) {
	workers := make([]chan StoredEvent, n)
	for i := range workers {
		workers[i] = make(chan StoredEvent, len(batch))
	}

	var (
		wg       sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
	)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i := range workers {
		wg.Add(1)
		go startWorker(parallelWorkerArgs{
			ctx:      ctx,
			ch:       workers[i],
			wc:       WorkerContext{ID: i, Count: n},
			errMu:    &errMu,
			firstErr: &firstErr,
			cancel:   cancel,
		}, &wg)
	}

	var dispatchErr error
	for _, ev := range batch {
		select {
		case workers[pickWorker(ev, len(workers))] <- ev:
		case <-ctx.Done():
			// The context was cancelled mid-dispatch. Stop handing out
			// events: reporting success here would advance the cursor
			// (or clean up events) for a batch whose remaining events
			// were never dispatched, silently losing them. Record the
			// cancellation and fall through to drain the workers we
			// already started so they don't leak.
			dispatchErr = ctx.Err()
		}
		if dispatchErr != nil {
			break
		}
	}
	for _, ch := range workers {
		close(ch)
	}
	wg.Wait()

	// Strict in the parallel path: per-EntityID partitioning means partial
	// per-worker progress can't be merged into a single cursor update
	// without risking lost events. Any error — a cancelled dispatch or a
	// handler/Commit failure — discards the whole batch so the next Run
	// retries it from the last committed position.
	if dispatchErr != nil {
		return 0, nil, dispatchErr
	}
	if firstErr != nil {
		return 0, nil, firstErr
	}
	return batch[len(batch)-1].IncrementID, batch, nil
}

// --- batchStrategy ---------------------------------------------------

// batchStrategy encapsulates how a batch of events is dispatched to
// handlers. A relay holds a single strategy chosen at construction time
// and is agnostic to which concrete implementation it runs:
// handlerBatchStrategy (plain Handler) or batchHandlerBatchStrategy
// (BatchHandler with a Commit barrier). Both execution paths live here so
// the relay only has to choose between them based on the configured
// parallelism.
type batchStrategy interface {
	// validate reports whether the strategy is ready to run. It returns
	// ErrNilFactory when the strategy was built with a nil handler factory,
	// so a relay can surface the misconfiguration from Run instead of
	// dereferencing nil mid-batch.
	validate() error
	// runSequential processes the whole batch on a single goroutine,
	// returning the last successfully processed IncrementID, the processed
	// events, and the first error (if any). It honours ctx cancellation
	// between events.
	runSequential(ctx context.Context, batch []StoredEvent) (int64, []StoredEvent, error)
	// startParallelWorker is launched once per worker goroutine by
	// runParallel; it drains the worker's channel and applies the
	// per-event logic.
	startParallelWorker(a parallelWorkerArgs, wg *sync.WaitGroup)
}

// --- handlerBatchStrategy --------------------------------------------

// handlerBatchStrategy powers the plain-Handler relays: partial progress
// in the sequential path, strict in the parallel path. The factory
// produces a plain Handler; Commit is irrelevant here so the strategy
// never calls it.
type handlerBatchStrategy struct {
	factory func(WorkerContext) Handler
}

func (s handlerBatchStrategy) validate() error {
	if s.factory == nil {
		return ErrNilFactory
	}
	return nil
}

func (s handlerBatchStrategy) runSequential(ctx context.Context, batch []StoredEvent) (int64, []StoredEvent, error) {
	handler := buildPlainHandler(s.factory, WorkerContext{ID: 0, Count: 1})
	var (
		processed []StoredEvent
		newLast   int64
	)
	for _, ev := range batch {
		// Honour cancellation between events: report partial progress so the
		// caller advances the cursor to the last successfully handled event.
		if err := ctx.Err(); err != nil {
			return newLast, processed, err
		}
		if err := handler.Handle(ctx, ev); err != nil {
			// Partial progress: caller may advance cursor to newLast.
			return newLast, processed, err
		}
		newLast = ev.IncrementID
		processed = append(processed, ev)
	}
	return newLast, processed, nil
}

func (s handlerBatchStrategy) startParallelWorker(a parallelWorkerArgs, wg *sync.WaitGroup) {
	defer wg.Done()
	var handler Handler
	sawEvent := false
	for ev := range a.ch {
		if !sawEvent {
			handler = buildPlainHandler(s.factory, a.wc)
		}
		sawEvent = true
		if err := handler.Handle(a.ctx, ev); err != nil {
			recordFirstErr(a.errMu, a.firstErr, err)
			a.cancel()
			return
		}
	}
}

// --- batchHandlerBatchStrategy ---------------------------------------

// batchHandlerBatchStrategy powers the BatchHandler relays: strict
// all-or-nothing on every path. Commit fires once per worker per batch
// after every routed event has been Handled. Any error — Handle, Commit,
// or cancellation — discards the partial progress.
type batchHandlerBatchStrategy struct {
	factory func(WorkerContext) BatchHandler
}

func (s batchHandlerBatchStrategy) validate() error {
	if s.factory == nil {
		return ErrNilFactory
	}
	return nil
}

func (s batchHandlerBatchStrategy) runSequential(ctx context.Context, batch []StoredEvent) (int64, []StoredEvent, error) {
	handler := buildBatchHandler(s.factory, WorkerContext{ID: 0, Count: 1})
	var (
		processed []StoredEvent
		newLast   int64
	)
	for _, ev := range batch {
		// Honour cancellation between events. The Commit barrier has not
		// fired yet, so the batch is not durably processed: discard it and
		// let the next Run retry.
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}
		if err := handler.Handle(ctx, ev); err != nil {
			return 0, nil, err
		}
		newLast = ev.IncrementID
		processed = append(processed, ev)
	}
	// Commit barrier: flushes per-batch work (e.g. AMQP publish) once
	// after every event in the batch was handled. A Commit failure
	// here means the barrier did not hold, so the batch is discarded.
	if err := handler.Commit(ctx); err != nil {
		return 0, nil, err
	}
	return newLast, processed, nil
}

func (s batchHandlerBatchStrategy) startParallelWorker(a parallelWorkerArgs, wg *sync.WaitGroup) {
	defer wg.Done()
	var handler BatchHandler
	sawEvent := false
	for ev := range a.ch {
		if !sawEvent {
			handler = buildBatchHandler(s.factory, a.wc)
		}
		sawEvent = true
		if err := handler.Handle(a.ctx, ev); err != nil {
			recordFirstErr(a.errMu, a.firstErr, err)
			a.cancel()
			return
		}
	}
	// Skip Commit on workers that never saw an event: a closed channel
	// with no dispatches means the worker had nothing to flush.
	if !sawEvent {
		return
	}
	if err := handler.Commit(a.ctx); err != nil {
		recordFirstErr(a.errMu, a.firstErr, err)
		a.cancel()
		return
	}
}

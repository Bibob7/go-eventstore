package eventstore

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"time"
)

// relayBase holds common state and logic shared across relay implementations.
type relayBase struct {
	name                  string
	mu                    sync.RWMutex
	handlerFactories      []func(WorkerContext) Handler
	batchHandlerFactories []func(WorkerContext) BatchHandler
	handleDelay           time.Duration
	parallelism           int
}

func (b *relayBase) registerHandlerFactory(factory func(WorkerContext) Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlerFactories = append(b.handlerFactories, factory)
}

func (b *relayBase) registerBatchHandler(factory func(WorkerContext) BatchHandler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.batchHandlerFactories = append(b.batchHandlerFactories, factory)
}

func (b *relayBase) handlerFactorySnapshot() []func(WorkerContext) Handler {
	b.mu.RLock()
	defer b.mu.RUnlock()
	snapshot := make([]func(WorkerContext) Handler, len(b.handlerFactories))
	copy(snapshot, b.handlerFactories)
	return snapshot
}

func (b *relayBase) batchHandlerFactorySnapshot() []func(WorkerContext) BatchHandler {
	b.mu.RLock()
	defer b.mu.RUnlock()
	snapshot := make([]func(WorkerContext) BatchHandler, len(b.batchHandlerFactories))
	copy(snapshot, b.batchHandlerFactories)
	return snapshot
}

func (b *relayBase) handleEvent(ctx context.Context, event StoredEvent, h Handler) error {
	handlerName := fmt.Sprintf("%s_%s", b.name, h.Name())
	if err := h.Handle(ctx, event); err != nil {
		if errors.Is(err, ErrEventNotReadyToProcess) {
			slog.Info("Event not ready to process, stopping", "handler_name", handlerName, "event_id", event.ID, "error", err)
			return err
		}
		slog.Error("Error relaying event", "handler_name", handlerName, "event_id", event.ID, "error", err)
		return err
	}
	return nil
}

func (b *relayBase) waitHandleDelay(ctx context.Context) error {
	if b.handleDelay <= 0 {
		return nil
	}
	slog.Debug("Delaying next event relay", "name", b.name, "delay", b.handleDelay)
	timer := time.NewTimer(b.handleDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		slog.Debug("Context done, stopping relay", "name", b.name)
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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
// Used by processBatch to collect the first handler error across workers.
func recordFirstErr(mu *sync.Mutex, dst *error, err error) {
	mu.Lock()
	defer mu.Unlock()
	if *dst == nil {
		*dst = err
	}
}

// processBatch is the shared inner loop used by pointerRelay.Run and
// transientRelay.Run. It returns:
//   - newLastIncrementID: the IncrementID of the last event that completed
//     every handler. Zero when nothing was processed.
//   - processed: the events that completed every handler, in original order.
//   - err: the first handler error, or ctx.Err().
//
// On parallelism <= 1 the loop is purely sequential and equivalent to the
// pre-pool behaviour: each registered factory is invoked once with
// WorkerContext{ID: 0, Count: 1}, then each event runs through every
// resulting handler. The partial-progress invariant is preserved (processed
// contains only events that fully completed).
//
// On parallelism > 1 events are dispatched across n goroutines via
// fnv32a(EntityID) % n. Each goroutine invokes every HandlerFactory and
// BatchHandlerFactory exactly once with its own WorkerContext, giving that
// worker private handler instances. BatchHandler.Commit is invoked once per
// worker inside its goroutine, before the WaitGroup is released — equivalent
// to the PHP MESSAGE_SYNC_ACK barrier. The whole batch is treated as
// all-or-nothing: on any error, processed is empty and newLastIncrementID
// is zero, so the caller will not advance the cursor or delete events.
func (b *relayBase) processBatch(ctx context.Context, batch []StoredEvent) (int64, []StoredEvent, error) {
	if b.parallelism <= 1 {
		return b.processSequential(ctx, batch)
	}
	return b.processParallel(ctx, batch)
}

// processSequential handles a single batch on the calling goroutine.
// Factories run exactly once with WorkerContext{ID: 0, Count: 1}, plain
// Handler returns are auto-wrapped via asBatchHandlers so the Commit loop
// stays uniform. On any error the partial-progress invariant is preserved
// (only events that fully completed every handler are returned in
// processed).
func (b *relayBase) processSequential(ctx context.Context, batch []StoredEvent) (int64, []StoredEvent, error) {
	seqBatchHandlers := asBatchHandlers(buildHandlers(b.handlerFactorySnapshot(), WorkerContext{ID: 0, Count: 1}))
	var processed []StoredEvent
	var newLast int64
	for _, ev := range batch {
		for _, bh := range seqBatchHandlers {
			if err := bh.Handle(ctx, ev); err != nil {
				return newLast, processed, err
			}
		}
		newLast = ev.IncrementID
		processed = append(processed, ev)
		if err := b.waitHandleDelay(ctx); err != nil {
			return newLast, processed, err
		}
		// Commit fires after the last event of the batch, so
		// per-worker state (e.g. an AMQP buffer) flushes once.
		if ev.IncrementID == batch[len(batch)-1].IncrementID {
			for _, bh := range seqBatchHandlers {
				if err := bh.Commit(ctx); err != nil {
					return newLast, processed, err
				}
			}
		}
	}
	return newLast, processed, nil
}

// processParallel dispatches the batch across b.parallelism worker
// goroutines, each reading from a private buffered channel. The dispatcher
// never blocks in the happy path (buffer = len(batch)); cancel-on-error
// drains via channel close. First error wins via recordFirstErr; the
// returned (newLast, processed) is all-or-nothing — empty on any failure.
func (b *relayBase) processParallel(ctx context.Context, batch []StoredEvent) (int64, []StoredEvent, error) {
	handlerFactories := b.handlerFactorySnapshot()
	batchFactories := b.batchHandlerFactorySnapshot()

	workers := make([]chan StoredEvent, b.parallelism)
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
		go b.runWorker(workerArgs{
			ctx:              ctx,
			workerID:         i,
			ch:               workers[i],
			ctxWorker:        WorkerContext{ID: i, Count: b.parallelism},
			handlerFactories: handlerFactories,
			batchFactories:   batchFactories,
			errMu:            &errMu,
			firstErr:         &firstErr,
			cancel:           cancel,
		}, &wg)
	}

	for _, ev := range batch {
		select {
		case workers[pickWorker(ev, len(workers))] <- ev:
		case <-ctx.Done():
		}
	}
	for _, ch := range workers {
		close(ch)
	}
	wg.Wait()

	if firstErr != nil {
		return 0, nil, firstErr
	}
	if len(batch) > 0 {
		return batch[len(batch)-1].IncrementID, batch, nil
	}
	return 0, nil, nil
}

// workerArgs bundles the inputs runWorker needs from processParallel.
// Grouping them in a struct keeps the runWorker signature short and makes
// it easy to read the call site.
type workerArgs struct {
	ctx              context.Context
	workerID         int
	ch               <-chan StoredEvent
	ctxWorker        WorkerContext
	handlerFactories []func(WorkerContext) Handler
	batchFactories   []func(WorkerContext) BatchHandler
	errMu            *sync.Mutex
	firstErr         *error
	cancel           context.CancelFunc
}

// runWorker is the body of a single parallel worker. It pulls events from
// its channel, builds its private handler slice lazily on the first event
// (so workers with no events skip both Handle and Commit and don't fire
// factory constructors unnecessarily), then runs Handle for every routed
// event. After the channel closes it invokes Commit once per BatchHandler
// in registration order — the PHP MESSAGE_SYNC barrier. Any error
// short-circuits the rest of the workers via cancel().
func (b *relayBase) runWorker(a workerArgs, wg *sync.WaitGroup) {
	defer wg.Done()
	var perWorkerHandlers []BatchHandler
	sawEvent := false
	for ev := range a.ch {
		if !sawEvent {
			perWorkerHandlers = append(perWorkerHandlers, asBatchHandlers(buildHandlers(a.handlerFactories, a.ctxWorker))...)
			perWorkerHandlers = append(perWorkerHandlers, buildBatchHandlers(a.batchFactories, a.ctxWorker)...)
		}
		sawEvent = true
		for _, h := range perWorkerHandlers {
			if err := h.Handle(a.ctx, ev); err != nil {
				recordFirstErr(a.errMu, a.firstErr, err)
				a.cancel()
				return
			}
		}
	}
	// Only invoke Commit on workers that actually processed at least one
	// event. Empty workers (closed channel that never received a dispatch)
	// would otherwise inflate commitCalls and trigger spurious flushes.
	if !sawEvent {
		return
	}
	for _, h := range perWorkerHandlers {
		if err := h.Commit(a.ctx); err != nil {
			recordFirstErr(a.errMu, a.firstErr, err)
			a.cancel()
			return
		}
	}
}

// buildBatchHandlers invokes each factory once with the given WorkerContext
// and returns the resulting BatchHandler slice in registration order. If no
// factories are registered, the slice is empty.
func buildBatchHandlers(factories []func(WorkerContext) BatchHandler, ctxWorker WorkerContext) []BatchHandler {
	if len(factories) == 0 {
		return nil
	}
	out := make([]BatchHandler, 0, len(factories))
	for _, f := range factories {
		out = append(out, f(ctxWorker))
	}
	return out
}

// buildHandlers invokes each factory once with the given WorkerContext and
// returns the resulting Handler slice in registration order. If no factories
// are registered, the slice is empty.
func buildHandlers(factories []func(WorkerContext) Handler, ctxWorker WorkerContext) []Handler {
	if len(factories) == 0 {
		return nil
	}
	out := make([]Handler, 0, len(factories))
	for _, f := range factories {
		out = append(out, f(ctxWorker))
	}
	return out
}

package eventstore

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"
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
	partitionStrategy     PartitionStrategy
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

// runParallel partitions the batch into n per-worker buckets using partitioner
// and runs one goroutine per non-empty worker via errgroup. Partitioning
// up front (rather than streaming events into per-worker channels) keeps
// each event on a stable worker for the lifetime of a Run, so any
// per-event ordering invariant the partitioner enforces is preserved
// within a worker. The runWorker closure is supplied by the concrete
// relay type — it knows how to invoke the registered factories and the
// per-event logic.
//
// errgroup gives us the worker pool's bookkeeping for free: Wait blocks until
// every worker drains, returns the first error any worker produced, and its
// derived context is cancelled on that first error so the remaining workers
// can bail out between events.
//
// Strict in the parallel path: per-event partitioning means partial
// per-worker progress can't be merged into a single cursor update without
// risking lost events. Any error — a cancelled context or a handler/Commit
// failure — discards the whole batch so the next Run retries it from the last
// committed position.
func runParallel(
	ctx context.Context,
	batch []StoredEvent,
	n int,
	partitioner PartitionStrategy,
	runWorker func(ctx context.Context, wc WorkerContext, events []StoredEvent) error,
) (int64, []StoredEvent, error) {
	// Nothing to process: return before indexing batch[len(batch)-1] below.
	// Mirrors runSequential, which no-ops on an empty batch.
	if len(batch) == 0 {
		return 0, nil, nil
	}

	workers := make([][]StoredEvent, n)
	if partitioner == nil {
		partitioner = DefaultPartitionStrategy
	}
	for _, ev := range batch {
		i := partitioner.Partition(ev, n)
		workers[i] = append(workers[i], ev)
	}

	g, ctx := errgroup.WithContext(ctx)
	for i, events := range workers {
		if len(events) == 0 {
			continue
		}
		g.Go(func() error {
			return runWorker(ctx, WorkerContext{ID: i, Count: n}, events)
		})
	}
	if err := g.Wait(); err != nil {
		return 0, nil, err
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
	// runWorker processes the events routed to worker wc.ID on a single
	// goroutine. runParallel launches it once per non-empty worker. It
	// honours ctx cancellation between events so a sibling worker's error
	// stops it promptly, and reports any handler/Commit failure.
	runWorker(ctx context.Context, wc WorkerContext, events []StoredEvent) error
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

func (s handlerBatchStrategy) runWorker(ctx context.Context, wc WorkerContext, events []StoredEvent) error {
	handler := buildPlainHandler(s.factory, wc)
	for _, ev := range events {
		// Honour cancellation between events so a sibling worker's error
		// (which errgroup turns into a ctx cancellation) stops us promptly.
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := handler.Handle(ctx, ev); err != nil {
			return err
		}
	}
	return nil
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

func (s batchHandlerBatchStrategy) runWorker(ctx context.Context, wc WorkerContext, events []StoredEvent) error {
	// runParallel never launches an empty worker, so there is always at
	// least one event to handle and a Commit barrier to fire.
	handler := buildBatchHandler(s.factory, wc)
	for _, ev := range events {
		// Honour cancellation between events. The Commit barrier has not
		// fired yet, so the batch is not durably processed: discard it and
		// let the next Run retry.
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := handler.Handle(ctx, ev); err != nil {
			return err
		}
	}
	// Commit barrier: flushes per-batch work (e.g. AMQP publish) once after
	// every routed event was handled. A Commit failure discards the batch.
	return handler.Commit(ctx)
}

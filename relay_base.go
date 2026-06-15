package eventstore

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"
)

// relayPersistTimeout bounds the detached write that records relay progress
// during shutdown, so a wedged database cannot block a relay indefinitely.
const relayPersistTimeout = 5 * time.Second

// detachedPersistCtx derives a context for persisting relay progress that is
// NOT cancelled when ctx is: events already processed must be acknowledged even
// when a graceful shutdown cancelled ctx, or the batch is re-delivered on
// restart. Parent values are retained; the deadline is relayPersistTimeout.
func detachedPersistCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), relayPersistTimeout)
}

// relayConfig carries the options collected from RelayOption. It is embedded by
// every concrete relay type.
type relayConfig struct {
	batchSize             int
	batchDelay            time.Duration
	conditionalBatchDelay time.Duration
	parallelism           int
	partitionStrategy     PartitionStrategy
}

// buildPlainHandler invokes the Handler factory for the given worker, or
// returns nil when no factory is configured.
func buildPlainHandler(factory func(WorkerContext) Handler, wc WorkerContext) Handler {
	if factory == nil {
		return nil
	}
	return factory(wc)
}

// buildBatchHandler invokes the BatchHandler factory for the given worker, or
// returns nil when no factory is configured.
func buildBatchHandler(factory func(WorkerContext) BatchHandler, wc WorkerContext) BatchHandler {
	if factory == nil {
		return nil
	}
	return factory(wc)
}

// runParallel partitions the batch into n per-worker buckets and runs one
// goroutine per non-empty worker via errgroup. Partitioning up front keeps each
// event on a stable worker, preserving any per-worker ordering the partitioner
// enforces. errgroup.Wait returns the first worker error and cancels the
// derived context so siblings can bail out between events.
//
// The parallel path is strict: per-event partitioning means partial per-worker
// progress can't be merged into one cursor update, so any error discards the
// whole batch for the next Run to retry.
func runParallel(
	ctx context.Context,
	batch []StoredEvent,
	n int,
	partitioner PartitionStrategy,
	runWorker func(ctx context.Context, wc WorkerContext, events []StoredEvent) error,
) (int64, []StoredEvent, error) {
	// Nothing to process: return before indexing batch[len(batch)-1] below.
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

// batchStrategy encapsulates how a batch is dispatched to handlers. A relay
// holds one strategy chosen at construction: handlerBatchStrategy (plain
// Handler) or batchHandlerBatchStrategy (BatchHandler with a Commit barrier).
type batchStrategy interface {
	// validate returns ErrNilFactory when the strategy was built with a nil
	// factory, so a relay can surface it from Run instead of panicking mid-batch.
	validate() error
	// runSequential processes the whole batch on one goroutine, returning the
	// last processed IncrementID, the processed events, and the first error. It
	// honours ctx cancellation between events.
	runSequential(ctx context.Context, batch []StoredEvent) (int64, []StoredEvent, error)
	// runWorker processes the events routed to worker wc.ID on one goroutine;
	// runParallel launches it once per non-empty worker.
	runWorker(ctx context.Context, wc WorkerContext, events []StoredEvent) error
}

// --- handlerBatchStrategy --------------------------------------------

// handlerBatchStrategy powers the plain-Handler relays: partial progress in the
// sequential path, strict in the parallel path. Commit is irrelevant here.
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
		// Honour cancellation; partial progress is reported so the caller can
		// advance the cursor to the last successfully handled event.
		if err := ctx.Err(); err != nil {
			return newLast, processed, err
		}
		if err := handler.Handle(ctx, ev); err != nil {
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
		// Honour cancellation so a sibling worker's error stops us promptly.
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
// all-or-nothing on every path. Commit fires once per worker after every routed
// event was handled; any error discards the batch.
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
		// The Commit barrier has not fired yet, so on cancellation the batch is
		// not durably processed: discard it and let the next Run retry.
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}
		if err := handler.Handle(ctx, ev); err != nil {
			return 0, nil, err
		}
		newLast = ev.IncrementID
		processed = append(processed, ev)
	}
	// Commit barrier: flush per-batch work (e.g. AMQP publish) once after every
	// event was handled. A failure here discards the batch.
	if err := handler.Commit(ctx); err != nil {
		return 0, nil, err
	}
	return newLast, processed, nil
}

func (s batchHandlerBatchStrategy) runWorker(ctx context.Context, wc WorkerContext, events []StoredEvent) error {
	handler := buildBatchHandler(s.factory, wc)
	for _, ev := range events {
		// The Commit barrier has not fired yet, so on cancellation the batch is
		// not durably processed: discard it and let the next Run retry.
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := handler.Handle(ctx, ev); err != nil {
			return err
		}
	}
	// Commit barrier: flush per-batch work once after every routed event was
	// handled. A failure discards the batch.
	return handler.Commit(ctx)
}

package eventstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

const (
	// DefaultWaitTime is the delay applied by a delayedRelay between batches
	// when ErrEventNotReadyToProcess is returned.
	DefaultWaitTime = 5 * time.Second
	// DefaultBatchSize is the number of events fetched per relay run when no
	// explicit batch size is configured via WithBatchSize.
	DefaultBatchSize = 100
)

var (
	// ErrIncrementIDConflict signals that a stored increment ID changed between
	// read and write, so the caller's expected previous value is no longer current.
	ErrIncrementIDConflict = errors.New("increment id conflict")
)

// pointerRelay is the cursor-based Relay implementation backed by a
// PointerStore + IncrementIDStore. It is agnostic to the handler kind:
// the batchStrategy chosen at construction time (plain Handler or
// BatchHandler) determines how each batch is dispatched.
type pointerRelay struct {
	relayConfig
	name             string
	eventStore       PointerStore
	incrementIDStore IncrementIDStore
	strategy         batchStrategy
}

// newPointerRelay applies opts, wires up the requested strategy, and
// wraps the relay in the configured delay decorators. It is shared by the
// plain-Handler and BatchHandler constructors.
func newPointerRelay(name string, store PointerStore, incrementIDStore IncrementIDStore, strategy batchStrategy, opts ...RelayOption) Relay {
	cfg := &relayConfig{batchSize: DefaultBatchSize, parallelism: 1}
	for _, opt := range opts {
		opt(cfg)
	}

	var relay Relay = &pointerRelay{
		relayConfig:      *cfg,
		name:             name,
		eventStore:       store,
		incrementIDStore: incrementIDStore,
		strategy:         strategy,
	}

	if cfg.conditionalBatchDelay > 0 {
		relay = newDelayedRelay(relay, cfg.conditionalBatchDelay)
	}

	if cfg.batchDelay > 0 {
		relay = newBatchDelayedRelay(relay, cfg.batchDelay)
	}

	return relay
}

// NewPointerHandlerRelay creates a cursor-based Relay that reads from
// store and tracks its position in incrementIDStore. The name must be
// unique across all relays sharing the same IncrementIDStore.
//
// factory produces one Handler instance per worker; within a Run it is
// invoked the first time a worker is handed an event, with a WorkerContext
// identifying that worker (ID in [0, Count)), so any per-worker state is
// fresh and not shared. Workers that receive no events never invoke the
// factory. With parallelism == 1 a single instance is built. If factory
// is nil, the first Run returns ErrNilFactory.
//
// Cursor semantics: partial progress is allowed in the sequential path
// (parallelism <= 1). If a Handle call fails mid-batch, the cursor
// advances up to the last successfully processed event so the next Run
// resumes from there. In the parallel path (parallelism > 1) the relay
// is strict all-or-nothing because the per-EntityID partitioning makes
// per-worker partial progress unsafe to merge into a single cursor
// update.
//
// The handler must be idempotent: the relay may re-deliver events in the
// partial-progress case (e.g. when the context is cancelled between two
// Handle calls and the cursor was advanced to the last successful event
// on the previous Run).
func NewPointerHandlerRelay(name string, store PointerStore, incrementIDStore IncrementIDStore, factory func(WorkerContext) Handler, opts ...RelayOption) Relay {
	return newPointerRelay(name, store, incrementIDStore, handlerBatchStrategy{factory: factory}, opts...)
}

// NewPointerBatchHandlerRelay creates a cursor-based Relay backed by
// BatchHandlers that reads from store and tracks its position in
// incrementIDStore. The name must be unique across all relays sharing
// the same IncrementIDStore.
//
// factory produces one BatchHandler instance per worker; within a Run it
// is invoked the first time a worker is handed an event, with a
// WorkerContext identifying that worker, so any per-worker state (e.g. an
// AMQP channel) is fresh and not shared. Workers that receive no events
// never invoke the factory. With parallelism == 1 a single instance is
// built. If factory is nil, the first Run returns ErrNilFactory.
//
// Strict all-or-nothing: the cursor is advanced only when the entire
// batch (Handle for every event plus Commit for every BatchHandler)
// completed successfully. Any failure leaves the cursor where it was
// and the next Run retries the same batch.
func NewPointerBatchHandlerRelay(name string, store PointerStore, incrementIDStore IncrementIDStore, factory func(WorkerContext) BatchHandler, opts ...RelayOption) Relay {
	return newPointerRelay(name, store, incrementIDStore, batchHandlerBatchStrategy{factory: factory}, opts...)
}

func (p *pointerRelay) Name() string {
	return p.name
}

func (p *pointerRelay) Run(ctx context.Context) (err error) {
	if err := p.strategy.validate(); err != nil {
		return err
	}
	lastIncrementID, err := p.incrementIDStore.GetIncrementID(ctx, p.name)
	if err != nil {
		return fmt.Errorf("failed to get last increment id: %w", err)
	}
	storedEvents, err := p.eventStore.FetchBatchOfEventsSince(ctx, lastIncrementID, p.batchSize)
	if err != nil {
		return fmt.Errorf("failed to fetch events: %w", err)
	}

	if len(storedEvents) == 0 {
		return nil
	}

	var (
		newLastIncrementID int64
		processed          bool
	)
	defer func() {
		if !processed {
			slog.Debug("No events relayed", "name", p.name, "last_increment_id", lastIncrementID)
			return
		}
		if setErr := p.incrementIDStore.SetIncrementID(ctx, p.name, lastIncrementID, newLastIncrementID); setErr != nil && err == nil {
			err = fmt.Errorf("failed to set new increment id: %w", setErr)
		}
	}()

	if p.parallelism <= 1 {
		newLastIncrementID, _, err = p.strategy.runSequential(ctx, storedEvents, p.handleDelay)
	} else {
		newLastIncrementID, _, err = runParallel(ctx, storedEvents, p.parallelism, p.strategy.startParallelWorker)
	}
	if newLastIncrementID > 0 {
		processed = true
	}
	if err != nil {
		return err
	}

	return nil
}

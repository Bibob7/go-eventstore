package eventstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

const (
	// DefaultWaitTime is the delay applied between batches when
	// ErrEventNotReadyToProcess is returned.
	DefaultWaitTime = 5 * time.Second
	// DefaultBatchSize is the number of events fetched per relay run when none
	// is configured via WithBatchSize.
	DefaultBatchSize = 100
)

var (
	// ErrIncrementIDConflict signals that a stored increment ID changed between
	// read and write, so the caller's expected previous value is no longer current.
	ErrIncrementIDConflict = errors.New("increment id conflict")
)

// pointerRelay is the cursor-based Relay backed by a PointerStore and
// IncrementIDStore. The strategy chosen at construction dispatches each batch.
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
	cfg := &relayConfig{batchSize: DefaultBatchSize, parallelism: 1, partitionStrategy: DefaultPartitionStrategy}
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

// NewPointerHandlerRelay creates a cursor-based Relay that reads from store and
// tracks its position in incrementIDStore. The name must be unique across all
// relays sharing the same IncrementIDStore. If factory is nil, the first Run
// returns ErrNilFactory.
//
// factory produces one Handler per worker (see WorkerContext). The sequential
// path (parallelism <= 1) allows partial progress: a mid-batch failure advances
// the cursor to the last handled event. The parallel path is strict
// all-or-nothing. Either way the handler must be idempotent, as events may be
// re-delivered after partial progress.
func NewPointerHandlerRelay(name string, store PointerStore, incrementIDStore IncrementIDStore, factory func(WorkerContext) Handler, opts ...RelayOption) Relay {
	return newPointerRelay(name, store, incrementIDStore, handlerBatchStrategy{factory: factory}, opts...)
}

// NewPointerBatchHandlerRelay creates a cursor-based Relay backed by
// BatchHandlers that reads from store and tracks its position in
// incrementIDStore. The name must be unique across all relays sharing the same
// IncrementIDStore. If factory is nil, the first Run returns ErrNilFactory.
//
// factory produces one BatchHandler per worker (see WorkerContext). Processing
// is strict all-or-nothing: the cursor advances only after Handle for every
// event and Commit for every BatchHandler succeed, otherwise the next Run
// retries the same batch.
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
		// Detach only on shutdown (ctx already cancelled) so the cursor write
		// still goes through; see detachedPersistCtx.
		persistCtx := ctx
		if ctx.Err() != nil {
			var cancel context.CancelFunc
			persistCtx, cancel = detachedPersistCtx(ctx)
			defer cancel()
		}
		if setErr := p.incrementIDStore.SetIncrementID(persistCtx, p.name, lastIncrementID, newLastIncrementID); setErr != nil && err == nil {
			err = fmt.Errorf("failed to set new increment id: %w", setErr)
		}
	}()

	if p.parallelism <= 1 {
		newLastIncrementID, _, err = p.strategy.runSequential(ctx, storedEvents)
	} else {
		newLastIncrementID, _, err = runParallel(ctx, storedEvents, p.parallelism, p.partitionStrategy, p.strategy.runWorker)
	}
	if newLastIncrementID > 0 {
		processed = true
	}
	if err != nil {
		return err
	}

	return nil
}

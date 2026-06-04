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
	// ErrEventNotReadyToProcess signals that an event cannot be handled yet.
	// Handlers should return this error to indicate a temporary condition;
	// the relay will pause before retrying rather than treating it as a hard failure.
	ErrEventNotReadyToProcess = errors.New("event not ready to process")
	// ErrIncrementIDConflict signals that a stored increment ID changed between
	// read and write, so the caller's expected previous value is no longer current.
	ErrIncrementIDConflict = errors.New("increment id conflict")
)

// Relay fetches events from a PointerStore and dispatches them to registered handlers.
// Use NewPointerRelay to create a Relay and call Run in a loop (e.g. via a ticker or worker pool).
type Relay interface {
	// Name returns the unique name of this relay, used as the consumer identifier.
	Name() string
	// RegisterHandlerFactory registers a factory that produces one Handler
	// instance per worker when the relay runs with WithParallelism(n > 1).
	// With n == 1 a single instance is built. The factory is invoked once per
	// worker at the start of each Run with a WorkerContext identifying that
	// worker (ID in [0, Count)), so any per-worker state (e.g. a counter or
	// a connection) is fresh and not shared. Calling RegisterHandlerFactory
	// multiple times appends factories; each factory's resulting Handler is
	// invoked in registration order within a worker.
	RegisterHandlerFactory(factory func(WorkerContext) Handler) Relay
	// RegisterBatchHandler registers a factory that produces one
	// BatchHandler instance per worker when the relay runs with
	// WithParallelism(n > 1). With n == 1 a single instance is built. The
	// factory is invoked once per worker at the start of each Run with a
	// WorkerContext identifying that worker, so any per-worker state (e.g.
	// an AMQP channel) is fresh and not shared. Calling RegisterBatchHandler
	// multiple times appends factories; each factory's resulting
	// BatchHandler is invoked in registration order within a worker.
	RegisterBatchHandler(factory func(WorkerContext) BatchHandler) Relay
	// Run fetches the next batch of events and dispatches them to all handlers.
	// It returns nil when the batch is empty or fully processed.
	Run(ctx context.Context) error
}

type pointerRelay struct {
	relayBase
	eventStore       PointerStore
	incrementIDStore IncrementIDStore
	batchSize        int
}

// NewPointerRelay creates a cursor-based Relay that reads from store and tracks
// its position in incrementIDStore. The name must be unique across all relays
// sharing the same IncrementIDStore.
func NewPointerRelay(name string, store PointerStore, incrementIDStore IncrementIDStore, opts ...RelayOption) Relay {
	cfg := &relayConfig{batchSize: DefaultBatchSize}
	for _, opt := range opts {
		opt(cfg)
	}

	p := &pointerRelay{
		relayBase:        relayBase{name: name, handleDelay: cfg.handleDelay, parallelism: cfg.parallelism},
		eventStore:       store,
		incrementIDStore: incrementIDStore,
		batchSize:        cfg.batchSize,
	}

	var relay Relay = p

	if cfg.conditionalBatchDelay > 0 {
		relay = newDelayedRelay(relay, cfg.conditionalBatchDelay)
	}

	if cfg.batchDelay > 0 {
		relay = newBatchDelayedRelay(relay, cfg.batchDelay)
	}

	return relay
}

func (p *pointerRelay) Name() string {
	return p.name
}

func (p *pointerRelay) RegisterHandlerFactory(factory func(WorkerContext) Handler) Relay {
	p.registerHandlerFactory(factory)
	return p
}

func (p *pointerRelay) RegisterBatchHandler(factory func(WorkerContext) BatchHandler) Relay {
	p.registerBatchHandler(factory)
	return p
}

func (p *pointerRelay) Run(ctx context.Context) (err error) {
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

	newLastIncrementID, _, err = p.processBatch(ctx, storedEvents)
	if newLastIncrementID > 0 {
		processed = true
	}
	if err != nil {
		return err
	}

	return nil
}

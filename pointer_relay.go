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
)

// Handler processes a single StoredEvent. Register one or more handlers on a
// Relay via RegisterHandler. All handlers are called for every event in order.
type Handler interface {
	// Handle processes of the given event. Return ErrEventNotReadyToProcess to
	// signal a temporary condition; return any other error to abort the batch.
	Handle(ctx context.Context, event StoredEvent) error
	// Name returns a stable, unique identifier for this handler.
	Name() string
}

// IncrementIDStore persists the last successfully processed IncrementID per relay.
// It is used to resume event processing after a restart without re-processing events.
type IncrementIDStore interface {
	// SetIncrementID stores the last processed IncrementID for the given consumer.
	SetIncrementID(ctx context.Context, consumerName string, incrementID int64) error
	// GetIncrementID returns the last processed IncrementID for the given consumer,
	// or 0 if no position has been recorded yet.
	GetIncrementID(ctx context.Context, consumerName string) (int64, error)
}

// Relay fetches events from a PointerStore and dispatches them to registered handlers.
// Use NewPointerRelay to create a Relay and call Run in a loop (e.g. via a ticker or worker pool).
type Relay interface {
	// Name returns the unique name of this relay, used as the consumer identifier.
	Name() string
	// RegisterHandler adds one or more handlers to the relay.
	// Handlers are called in registration order for every event.
	RegisterHandler(handler ...Handler) Relay
	// Run fetches the next batch of events and dispatches them to all handlers.
	// It returns nil when the batch is empty or fully processed.
	Run(ctx context.Context) error
}

type pointerRelay struct {
	eventStore       PointerStore
	incrementIDStore IncrementIDStore
	handler          []Handler
	name             string
	batchSize        int
	handleDelay      time.Duration
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
		name:             name,
		eventStore:       store,
		incrementIDStore: incrementIDStore,
		batchSize:        cfg.batchSize,
		handleDelay:      cfg.handleDelay,
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

func (p *pointerRelay) RegisterHandler(handler ...Handler) Relay {
	p.handler = append(p.handler, handler...)
	return p
}

func (p *pointerRelay) Run(ctx context.Context) error {
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

	var newLastIncrementID int64
	for _, storedEvent := range storedEvents {
		for _, handler := range p.handler {
			err = p.handleEvent(ctx, storedEvent, handler)
			if err != nil {
				return err
			}
		}
		newLastIncrementID = storedEvent.IncrementID
		if err := p.waitHandleDelay(ctx); err != nil {
			return err
		}
	}

	if newLastIncrementID == 0 {
		slog.Debug("No events relayed", "name", p.name, "last_increment_id", lastIncrementID)
		return nil
	}

	if err := p.incrementIDStore.SetIncrementID(ctx, p.name, newLastIncrementID); err != nil {
		return fmt.Errorf("failed to set new increment id: %w", err)
	}
	return nil
}

func (p *pointerRelay) waitHandleDelay(ctx context.Context) error {
	if p.handleDelay <= 0 {
		return nil
	}
	slog.Debug("Delaying next event relay", "name", p.name, "delay", p.handleDelay)
	select {
	case <-ctx.Done():
		slog.Debug("Context done, stopping relay", "name", p.name)
		return ctx.Err()
	case <-time.After(p.handleDelay):
		return nil
	}
}

func (p *pointerRelay) handleEvent(ctx context.Context, storedEvent StoredEvent, handler Handler) error {
	handlerName := fmt.Sprintf("%s_%s", p.name, handler.Name())

	if err := handler.Handle(ctx, storedEvent); err != nil {
		if errors.Is(err, ErrEventNotReadyToProcess) {
			slog.Info("Event not ready to process, stopping", "handler_name", handlerName, "event_id", storedEvent.ID, "error", err)
			return err
		}
		slog.Error("Error relaying event", "handler_name", handlerName, "event_id", storedEvent.ID, "error", err)
		return err
	}
	return nil
}

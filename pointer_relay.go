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

// RelayOption is a functional option for configuring a PointerRelay.
type RelayOption func(*pointerRelay)

// WithBatchSize sets the maximum number of events fetched per relay run.
// Defaults to DefaultBatchSize.
func WithBatchSize(batchSize int) RelayOption {
	return func(p *pointerRelay) {
		p.batchSize = batchSize
	}
}

// WithHandleDelay inserts a pause between processing individual events within a batch.
// Useful for rate-limiting or giving downstream systems time to react.
func WithHandleDelay(delay time.Duration) RelayOption {
	return func(p *pointerRelay) {
		p.handleDelay = delay
	}
}

// WithBatchDelay sets an unconditional delay between every relay run.
// The relay waits this duration after each batch, regardless of the result.
func WithBatchDelay(d time.Duration) RelayOption {
	return func(p *pointerRelay) {
		p.batchDelay = d
	}
}

// WithConditionalBatchDelay sets a delay between relay runs when no events are ready.
// When ErrEventNotReadyToProcess is returned, the relay waits this duration
// before the next run. Defaults to DefaultWaitTime.
func WithConditionalBatchDelay(d time.Duration) RelayOption {
	return func(p *pointerRelay) {
		p.conditionalBatchDelay = d
	}
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
	eventStore            PointerStore
	incrementIDStore      IncrementIDStore
	handler               []Handler
	name                  string
	batchSize             int
	handleDelay           time.Duration
	batchDelay            time.Duration
	conditionalBatchDelay time.Duration
}

// NewPointerRelay creates a cursor-based Relay that reads from store and tracks
// its position in incrementIDStore. The name must be unique across all relays
// sharing the same IncrementIDStore.
func NewPointerRelay(name string, store PointerStore, incrementIDStore IncrementIDStore, opts ...RelayOption) Relay {
	p := &pointerRelay{
		name:             name,
		eventStore:       store,
		incrementIDStore: incrementIDStore,
		batchSize:        DefaultBatchSize,
	}
	for _, opt := range opts {
		opt(p)
	}

	var relay Relay = p

	if p.conditionalBatchDelay > 0 {
		relay = newDelayedRelay(relay, p.conditionalBatchDelay)
	}

	if p.batchDelay > 0 {
		relay = newBatchDelayedRelay(relay, p.batchDelay)
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

// delayedRelay is a wrapper around Relay that delays the next batch of events relay.
// It delays the execution of not yet ready events by a specified wait time.
type delayedRelay struct {
	relay    Relay
	waitTime time.Duration
}

func newDelayedRelay(relay Relay, waitTime time.Duration) Relay {
	if waitTime <= 0 {
		waitTime = DefaultWaitTime
	}
	return &delayedRelay{
		relay:    relay,
		waitTime: waitTime,
	}
}

func (r delayedRelay) Name() string {
	return r.relay.Name()
}

func (r delayedRelay) RegisterHandler(handler ...Handler) Relay {
	r.relay.RegisterHandler(handler...)
	return r
}

func (r delayedRelay) Run(ctx context.Context) error {
	err := r.relay.Run(ctx)
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrEventNotReadyToProcess) {
		slog.Info("Events relayed, delaying next batch because of ErrEventNotReadyToProcess", "name", r.relay.Name(), "wait_time", r.waitTime)
		select {
		case <-ctx.Done():
			slog.Debug("Context done, stopping delayed relay", "name", r.relay.Name())
			return ctx.Err()
		case <-time.After(r.waitTime):
			return nil
		}
	}
	return err
}

// batchDelayedRelay is a wrapper around Relay that adds an unconditional delay
// after every batch run, regardless of the result.
type batchDelayedRelay struct {
	relay      Relay
	batchDelay time.Duration
}

func newBatchDelayedRelay(relay Relay, batchDelay time.Duration) Relay {
	return &batchDelayedRelay{
		relay:      relay,
		batchDelay: batchDelay,
	}
}

func (r batchDelayedRelay) Name() string {
	return r.relay.Name()
}

func (r batchDelayedRelay) RegisterHandler(handler ...Handler) Relay {
	r.relay.RegisterHandler(handler...)
	return r
}

func (r batchDelayedRelay) Run(ctx context.Context) error {
	err := r.relay.Run(ctx)
	slog.Debug("Delaying next batch", "name", r.relay.Name(), "batch_delay", r.batchDelay)
	select {
	case <-ctx.Done():
		slog.Debug("Context done, stopping batch delayed relay", "name", r.relay.Name())
		return ctx.Err()
	case <-time.After(r.batchDelay):
	}
	return err
}

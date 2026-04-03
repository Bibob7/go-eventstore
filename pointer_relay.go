package eventstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

const (
	DefaultWaitTime  = 5 * time.Second
	DefaultBatchSize = 100
)

var (
	ErrEventNotReadyToProcess = errors.New("event not ready to process")
)

type IdempotencyRegistry interface {
	// RegisterKey registers an entry for the given key and namespace with state "pending".
	// It returns ErrAlreadyExist if the combination is already registered.
	RegisterKey(ctx context.Context, key, namespace string) error
	// MarkAsSuccess marks an entry as "success".
	MarkAsSuccess(ctx context.Context, key, namespace string) error
	// MarkAsFailed marks an entry as "failed".
	// In case of state failed, the event can be retried.
	MarkAsFailed(ctx context.Context, key, namespace string) error
}

type Handler interface {
	// Handle processes of the given eventstore.
	// It returns an error if the event cannot be processed.
	// If the event is not ready to be processed, it should return ErrEventNotReadyToProcess.
	Handle(ctx context.Context, event StoredEvent) error
	// Name returns the unique name of the handler.
	Name() string
}

type RelayOption func(*pointerRelay)

func WithBatchSize(batchSize int) RelayOption {
	return func(p *pointerRelay) {
		p.batchSize = batchSize
	}
}

func WithHandleDelay(delay time.Duration) RelayOption {
	return func(p *pointerRelay) {
		p.handleDelay = delay
	}
}

func WithBatchDelay(d time.Duration) RelayOption {
	return func(p *pointerRelay) {
		p.batchDelay = d
	}
}

func WithIdempotencyRegistry(registry IdempotencyRegistry) RelayOption {
	return func(p *pointerRelay) {
		p.idempotencyRegistry = registry
	}
}

type IncrementIDStore interface {
	SetIncrementID(ctx context.Context, consumerName string, incrementID int64) error
	GetIncrementID(ctx context.Context, consumerName string) (int64, error)
}

type Relay interface {
	Name() string
	RegisterHandler(handler ...Handler) Relay
	Run(ctx context.Context) error
}

type pointerRelay struct {
	eventStore          PointerStore
	incrementIDStore    IncrementIDStore
	idempotencyRegistry IdempotencyRegistry
	handler             []Handler
	name                string
	batchSize           int
	handleDelay         time.Duration
	batchDelay          time.Duration
}

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

	if p.batchDelay > 0 {
		relay = newDelayedRelay(relay, p.batchDelay)
	}

	return relay
}

func (p *pointerRelay) Name() string {
	return p.name
}

func (p *pointerRelay) RegisterHandler(handler ...Handler) Relay {
	for _, h := range handler {
		if p.idempotencyRegistry != nil {
			h = newIdempotentHandler(h, p.idempotencyRegistry, p.name)
		}
		p.handler = append(p.handler, h)
	}
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

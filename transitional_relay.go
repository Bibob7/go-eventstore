package eventstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

type transitionalRelay struct {
	store       CleanUpStore
	handler     []Handler
	name        string
	batchSize   int
	handleDelay time.Duration
}

// NewTransitionalRelay creates a Relay that fetches events from store, dispatches them
// to registered handlers, and removes each event after successful handling.
// The name must be unique and is used for logging. Options are shared with NewPointerRelay.
func NewTransitionalRelay(name string, store CleanUpStore, opts ...RelayOption) Relay {
	cfg := &relayConfig{batchSize: DefaultBatchSize}
	for _, opt := range opts {
		opt(cfg)
	}

	t := &transitionalRelay{
		name:        name,
		store:       store,
		batchSize:   cfg.batchSize,
		handleDelay: cfg.handleDelay,
	}

	var relay Relay = t

	if cfg.conditionalBatchDelay > 0 {
		relay = newDelayedRelay(relay, cfg.conditionalBatchDelay)
	}

	if cfg.batchDelay > 0 {
		relay = newBatchDelayedRelay(relay, cfg.batchDelay)
	}

	return relay
}

func (t *transitionalRelay) Name() string {
	return t.name
}

func (t *transitionalRelay) RegisterHandler(handler ...Handler) Relay {
	t.handler = append(t.handler, handler...)
	return t
}

func (t *transitionalRelay) Run(ctx context.Context) error {
	events, err := t.store.FetchBatchOfEvents(ctx, t.batchSize)
	if err != nil {
		return fmt.Errorf("failed to fetch events: %w", err)
	}

	for _, event := range events {
		for _, handler := range t.handler {
			if err := t.handleEvent(ctx, event, handler); err != nil {
				return err
			}
		}

		if err := t.store.CleanUpEvents(ctx, []StoredEvent{event}); err != nil {
			return fmt.Errorf("failed to clean up event %s: %w", event.ID, err)
		}

		if err := t.waitHandleDelay(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (t *transitionalRelay) waitHandleDelay(ctx context.Context) error {
	if t.handleDelay <= 0 {
		return nil
	}
	slog.Debug("Delaying next event relay", "name", t.name, "delay", t.handleDelay)
	select {
	case <-ctx.Done():
		slog.Debug("Context done, stopping relay", "name", t.name)
		return ctx.Err()
	case <-time.After(t.handleDelay):
		return nil
	}
}

func (t *transitionalRelay) handleEvent(ctx context.Context, event StoredEvent, handler Handler) error {
	handlerName := fmt.Sprintf("%s_%s", t.name, handler.Name())

	if err := handler.Handle(ctx, event); err != nil {
		if errors.Is(err, ErrEventNotReadyToProcess) {
			slog.Info("Event not ready to process, stopping", "handler_name", handlerName, "event_id", event.ID, "error", err)
			return err
		}
		slog.Error("Error relaying event", "handler_name", handlerName, "event_id", event.ID, "error", err)
		return err
	}
	return nil
}
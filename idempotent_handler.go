package eventstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

var (
	ErrAlreadyExist = errors.New("eventID for handler already exists")
)

// idempotentHandler is a Handler decorator that wraps another Handler with
// idempotency checks using an IdempotencyRegistry. It ensures that a given
// event is only processed once per handler.
type idempotentHandler struct {
	inner    Handler
	registry IdempotencyRegistry
	prefix   string // relay name, used to build the composite key
}

func newIdempotentHandler(inner Handler, registry IdempotencyRegistry, prefix string) Handler {
	return &idempotentHandler{
		inner:    inner,
		registry: registry,
		prefix:   prefix,
	}
}

func (h *idempotentHandler) Name() string {
	return h.inner.Name()
}

func (h *idempotentHandler) Handle(ctx context.Context, event StoredEvent) error {
	key := event.ID.String()
	namespace := h.prefix + "_" + h.inner.Name()

	err := h.registry.RegisterKey(ctx, key, namespace)
	if err != nil {
		if errors.Is(err, ErrAlreadyExist) {
			slog.Info(
				"Event already relayed, skipping for relay and handler",
				"relay_name", h.prefix,
				"handler_name", h.inner.Name(),
				"event_id", event.ID)
			return nil
		}
		slog.Error("Failed to register idempotency key", "error", err)
		return fmt.Errorf("failed to register idempotency key: %w", err)
	}

	if err = h.inner.Handle(ctx, event); err != nil {
		slog.Debug("Mark Event as failed", "handler_name", h.inner.Name(), "event_id", event.ID)
		if markErr := h.registry.MarkAsFailed(ctx, key, namespace); markErr != nil {
			slog.Error("Failed to mark idempotency key as failed", "error", markErr)
		}
		return err
	}

	slog.Debug("Mark Event as success", "handler_name", h.inner.Name(), "event_id", event.ID)
	if markErr := h.registry.MarkAsSuccess(ctx, key, namespace); markErr != nil {
		slog.Error("Failed to mark idempotency key as success", "error", markErr)
	}
	return nil
}

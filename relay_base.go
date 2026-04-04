package eventstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// relayBase holds common state and logic shared across relay implementations.
type relayBase struct {
	name        string
	handler     []Handler
	handleDelay time.Duration
}

func (b *relayBase) handleEvent(ctx context.Context, event StoredEvent, h Handler) error {
	handlerName := fmt.Sprintf("%s_%s", b.name, h.Name())
	if err := h.Handle(ctx, event); err != nil {
		if errors.Is(err, ErrEventNotReadyToProcess) {
			slog.Info("Event not ready to process, stopping", "handler_name", handlerName, "event_id", event.ID, "error", err)
			return err
		}
		slog.Error("Error relaying event", "handler_name", handlerName, "event_id", event.ID, "error", err)
		return err
	}
	return nil
}

func (b *relayBase) waitHandleDelay(ctx context.Context) error {
	if b.handleDelay <= 0 {
		return nil
	}
	slog.Debug("Delaying next event relay", "name", b.name, "delay", b.handleDelay)
	select {
	case <-ctx.Done():
		slog.Debug("Context done, stopping relay", "name", b.name)
		return ctx.Err()
	case <-time.After(b.handleDelay):
		return nil
	}
}
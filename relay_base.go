package eventstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// relayBase holds common state and logic shared across relay implementations.
type relayBase struct {
	name        string
	mu          sync.RWMutex
	handler     []Handler
	handleDelay time.Duration
}

func (b *relayBase) registerHandler(handler ...Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handler = append(b.handler, handler...)
}

func (b *relayBase) handlers() []Handler {
	b.mu.RLock()
	defer b.mu.RUnlock()
	snapshot := make([]Handler, len(b.handler))
	copy(snapshot, b.handler)
	return snapshot
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
	timer := time.NewTimer(b.handleDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		slog.Debug("Context done, stopping relay", "name", b.name)
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

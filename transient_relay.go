package eventstore

import (
	"context"
	"fmt"
)

type transientRelay struct {
	relayBase
	store     CleanUpStore
	batchSize int
}

// NewTransientRelay creates a Relay that fetches events from store, dispatches them
// to registered handlers, and removes each event after successful handling.
// The name must be unique and is used for logging. Options are shared with NewPointerRelay.
func NewTransientRelay(name string, store CleanUpStore, opts ...RelayOption) Relay {
	cfg := &relayConfig{batchSize: DefaultBatchSize}
	for _, opt := range opts {
		opt(cfg)
	}

	t := &transientRelay{
		relayBase: relayBase{name: name, handleDelay: cfg.handleDelay},
		store:     store,
		batchSize: cfg.batchSize,
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

func (t *transientRelay) Name() string {
	return t.name
}

func (t *transientRelay) RegisterHandler(handler ...Handler) Relay {
	t.registerHandler(handler...)
	return t
}

func (t *transientRelay) Run(ctx context.Context) (err error) {
	events, err := t.store.FetchBatchOfEvents(ctx, t.batchSize)
	if err != nil {
		return fmt.Errorf("failed to fetch events: %w", err)
	}

	processed := make([]StoredEvent, 0, len(events))
	defer func() {
		if len(processed) == 0 {
			return
		}
		if cleanUpErr := t.store.CleanUpEvents(ctx, processed); cleanUpErr != nil && err == nil {
			err = fmt.Errorf("failed to clean up events: %w", cleanUpErr)
		}
	}()

	for _, event := range events {
		for _, handler := range t.handlers() {
			if err = t.handleEvent(ctx, event, handler); err != nil {
				return err
			}
		}

		processed = append(processed, event)

		if err = t.waitHandleDelay(ctx); err != nil {
			return err
		}
	}

	return nil
}

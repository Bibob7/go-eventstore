package eventstore

import (
	"context"
	"fmt"
)

// transientRelay is the Relay implementation backed by a TransientStore.
// Events that complete successfully are removed from the store via
// CleanUpEvents. It is agnostic to the handler kind: the batchStrategy
// chosen at construction time (plain Handler or BatchHandler) determines
// how each batch is dispatched.
type transientRelay struct {
	relayConfig
	name     string
	store    TransientStore
	strategy batchStrategy
}

// newTransientRelay applies opts, wires up the requested strategy, and
// wraps the relay in the configured delay decorators. It is shared by the
// plain-Handler and BatchHandler constructors.
func newTransientRelay(name string, store TransientStore, strategy batchStrategy, opts ...RelayOption) Relay {
	cfg := &relayConfig{batchSize: DefaultBatchSize, parallelism: 1}
	for _, opt := range opts {
		opt(cfg)
	}

	var relay Relay = &transientRelay{
		relayConfig: *cfg,
		name:        name,
		store:       store,
		strategy:    strategy,
	}

	if cfg.conditionalBatchDelay > 0 {
		relay = newDelayedRelay(relay, cfg.conditionalBatchDelay)
	}

	if cfg.batchDelay > 0 {
		relay = newBatchDelayedRelay(relay, cfg.batchDelay)
	}

	return relay
}

// NewTransientHandlerRelay creates a Relay that fetches events from
// store, dispatches them to the handler the factory produces, and
// removes each event after successful handling.
//
// factory produces one Handler instance per worker; within a Run it is
// invoked the first time a worker is handed an event, with a
// WorkerContext. Workers that receive no events never invoke the factory.
// With parallelism == 1 a single instance is built. If factory is nil,
// the first Run returns ErrNilFactory.
//
// The handler must be idempotent: the relay may re-deliver events in the
// partial-progress case.
func NewTransientHandlerRelay(name string, store TransientStore, factory func(WorkerContext) Handler, opts ...RelayOption) Relay {
	return newTransientRelay(name, store, handlerBatchStrategy{factory: factory}, opts...)
}

// NewTransientBatchHandlerRelay creates a Relay backed by BatchHandlers
// that fetches events from store, dispatches them, and removes the entire
// batch after the Commit barrier succeeds for every routed event.
//
// factory produces one BatchHandler instance per worker; within a Run it
// is invoked the first time a worker is handed an event, with a
// WorkerContext. Workers that receive no events never invoke the factory.
// With parallelism == 1 a single instance is built. If factory is nil,
// the first Run returns ErrNilFactory.
//
// Strict all-or-nothing: any error leaves the batch in the store and
// the next Run retries it.
func NewTransientBatchHandlerRelay(name string, store TransientStore, factory func(WorkerContext) BatchHandler, opts ...RelayOption) Relay {
	return newTransientRelay(name, store, batchHandlerBatchStrategy{factory: factory}, opts...)
}

func (t *transientRelay) Name() string {
	return t.name
}

func (t *transientRelay) Run(ctx context.Context) (err error) {
	if err := t.strategy.validate(); err != nil {
		return err
	}
	events, err := t.store.FetchBatchOfEvents(ctx, t.batchSize)
	if err != nil {
		return fmt.Errorf("failed to fetch events: %w", err)
	}

	var processed []StoredEvent
	defer func() {
		if len(processed) == 0 {
			return
		}
		if cleanUpErr := t.store.CleanUpEvents(ctx, processed); cleanUpErr != nil && err == nil {
			err = fmt.Errorf("failed to clean up events: %w", cleanUpErr)
		}
	}()

	if t.parallelism <= 1 {
		_, processed, err = t.strategy.runSequential(ctx, events, t.handleDelay)
	} else {
		_, processed, err = runParallel(ctx, events, t.parallelism, t.strategy.startParallelWorker)
	}
	return err
}

package eventstore

import (
	"context"
	"fmt"
)

// transientRelay is the Relay backed by a TransientStore: successfully
// processed events are removed via CleanUpEvents. The strategy chosen at
// construction dispatches each batch.
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
	cfg := &relayConfig{batchSize: DefaultBatchSize, parallelism: 1, partitionStrategy: DefaultPartitionStrategy}
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

// NewTransientHandlerRelay creates a Relay that fetches events from store,
// dispatches them to the handler the factory produces, and removes each event
// after successful handling. If factory is nil, the first Run returns
// ErrNilFactory.
//
// factory produces one Handler per worker (see WorkerContext). The handler must
// be idempotent, as events may be re-delivered after partial progress.
func NewTransientHandlerRelay(name string, store TransientStore, factory func(WorkerContext) Handler, opts ...RelayOption) Relay {
	return newTransientRelay(name, store, handlerBatchStrategy{factory: factory}, opts...)
}

// NewTransientBatchHandlerRelay creates a Relay backed by BatchHandlers that
// fetches events from store, dispatches them, and removes the whole batch once
// the Commit barrier succeeds for every routed event. If factory is nil, the
// first Run returns ErrNilFactory.
//
// factory produces one BatchHandler per worker (see WorkerContext). Processing
// is strict all-or-nothing: any error leaves the batch in the store for the
// next Run to retry.
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
		// Detach only on shutdown (ctx already cancelled) so the cleanup still
		// goes through; see detachedPersistCtx.
		cleanupCtx := ctx
		if ctx.Err() != nil {
			var cancel context.CancelFunc
			cleanupCtx, cancel = detachedPersistCtx(ctx)
			defer cancel()
		}
		if cleanUpErr := t.store.CleanUpEvents(cleanupCtx, processed); cleanUpErr != nil && err == nil {
			err = fmt.Errorf("failed to clean up events: %w", cleanUpErr)
		}
	}()

	if t.parallelism <= 1 {
		_, processed, err = t.strategy.runSequential(ctx, events)
	} else {
		_, processed, err = runParallel(ctx, events, t.parallelism, t.partitionStrategy, t.strategy.runWorker)
	}
	return err
}

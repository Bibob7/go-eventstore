package eventstore

import (
	"context"
	"errors"
)

var (
	// ErrEventNotReadyToProcess signals that an event cannot be handled yet.
	// Handlers return it to pause the relay before retrying rather than failing
	// the batch.
	ErrEventNotReadyToProcess = errors.New("event not ready to process")
	// ErrNilFactory is returned by Run when the relay was created with a nil
	// handler factory.
	ErrNilFactory = errors.New("relay handler factory must not be nil")
)

// Relay fetches events from a store and dispatches them to handlers produced by
// a factory. Create one with a constructor (NewPointerHandlerRelay,
// NewTransientBatchHandlerRelay, …) and call Run in a loop.
type Relay interface {
	Name() string
	// Run fetches the next batch of events and dispatches them to the handler,
	// returning nil when the batch is empty or fully processed.
	//
	// Run is NOT safe to call concurrently for the same relay (or for two
	// relays sharing a name): overlapping Runs read the same cursor and deliver
	// the same events twice. Call Run sequentially and use WithParallelism to
	// fan out a single batch across goroutines under one cursor.
	Run(ctx context.Context) error
}

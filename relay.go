package eventstore

import (
	"context"
	"errors"
)

var (
	// ErrEventNotReadyToProcess signals that an event cannot be handled yet.
	// Handlers should return this error to indicate a temporary condition;
	// the relay will pause before retrying rather than treating it as a hard failure.
	ErrEventNotReadyToProcess = errors.New("event not ready to process")
	// ErrNilFactory is returned by Run when the relay was created with a nil
	// handler factory.
	ErrNilFactory = errors.New("relay handler factory must not be nil")
)

// Relay fetches events from a store and dispatches them to handlers
// produced by a single factory. All concrete relays — cursor-based
// (PointerStore + IncrementIDStore) and transient (TransientStore),
// plain Handler and BatchHandler — share this interface; they differ
// only in the factory type they accept and the strategy they run.
//
// Create a relay with one of the constructors and call Run in a loop
// (e.g. via a ticker or worker pool):
//
//   - NewPointerHandlerRelay / NewTransientHandlerRelay take a plain
//     Handler factory (handlerBatchStrategy): partial progress in the
//     sequential path, strict all-or-nothing in the parallel path.
//   - NewPointerBatchHandlerRelay / NewTransientBatchHandlerRelay take a
//     BatchHandler factory (batchHandlerBatchStrategy): strict
//     all-or-nothing on every path, with a per-worker Commit barrier.
type Relay interface {
	Name() string
	// Run fetches the next batch of events and dispatches them to the
	// handler. It returns nil when the batch is empty or fully processed.
	Run(ctx context.Context) error
}

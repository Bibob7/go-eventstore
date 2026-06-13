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
	//
	// Run is NOT safe to call concurrently for the same relay. A
	// cursor-based relay reads its position, fetches events from it, and
	// only then advances it, so two overlapping Runs — or two relay
	// instances that share the same name — read the same cursor and
	// deliver the same events to every caller (duplicate processing); the
	// SetIncrementID compare-and-swap only protects the stored cursor
	// value, not the redundant work. Likewise, running several instances
	// of a transient relay against one store competes for the same rows.
	// Call Run sequentially in a loop (e.g. via a ticker). To process a
	// single batch across multiple goroutines, use WithParallelism, which
	// fans out within one Run while keeping a single cursor.
	Run(ctx context.Context) error
}

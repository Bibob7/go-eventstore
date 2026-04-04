package eventstore

import "context"

// Handler processes a single StoredEvent. Register one or more handlers on a
// Relay via RegisterHandler. All handlers are called for every event in order.
type Handler interface {
	// Handle processes of the given event. Return ErrEventNotReadyToProcess to
	// signal a temporary condition; return any other error to abort the batch.
	Handle(ctx context.Context, event StoredEvent) error
	// Name returns a stable, unique identifier for this handler.
	Name() string
}

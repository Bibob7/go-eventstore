package eventstore

import "context"

// Handler processes a single StoredEvent. Plain-Handler relays
// (NewPointerHandlerRelay / NewTransientHandlerRelay) build one Handler per
// worker from a factory.
type Handler interface {
	// Handle processes the given event. Return ErrEventNotReadyToProcess to
	// signal a temporary condition; any other error aborts the batch.
	Handle(ctx context.Context, event StoredEvent) error
	// Name returns a stable, unique identifier for this handler.
	Name() string
}

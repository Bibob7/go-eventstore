package eventstore

import "context"

// Handler processes a single StoredEvent. A plain-Handler relay
// (NewPointerHandlerRelay / NewTransientHandlerRelay) is created with a
// single factory that produces one Handler per worker; the handler is
// called for every event on the worker that the event's EntityID hashes
// to.
type Handler interface {
	// Handle processes of the given event. Return ErrEventNotReadyToProcess to
	// signal a temporary condition; return any other error to abort the batch.
	Handle(ctx context.Context, event StoredEvent) error
	// Name returns a stable, unique identifier for this handler.
	Name() string
}

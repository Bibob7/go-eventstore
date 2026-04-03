package eventstore

import "context"

// Filter processes a slice of StoredEvents and returns a filtered subset.
// Implementations can apply ordering, gap detection, or other logic
// before events are handed to relay handlers.
type Filter interface {
	// Execute applies the filter to the given events and returns the result.
	// It may return fewer events than provided, but never more.
	Execute(ctx context.Context, storedEvents []StoredEvent) ([]StoredEvent, error)
}

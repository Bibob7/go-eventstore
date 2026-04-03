package eventstore

import (
	"context"
)

// Store is the minimal write interface for appending domain events.
type Store interface {
	// Append persists one or more domain events to the store.
	Append(ctx context.Context, events ...DomainEvent) error
}

// CleanUpStore extends Store with the ability to fetch and remove
// already-processed events, for use in outbox cleanup patterns.
type CleanUpStore interface {
	Store
	// FetchBatchOfEvents returns up to limit events starting from the smallest IncrementID.
	FetchBatchOfEvents(ctx context.Context, limit int) ([]StoredEvent, error)
	// CleanUpEvents removes the given events from the store.
	CleanUpEvents(ctx context.Context, events []StoredEvent) error
}

// PointerStore extends Store with cursor-based event fetching.
// Implementations track a position (IncrementID) and return events after it,
// enabling relay consumers to process events exactly once.
type PointerStore interface {
	Store
	// FetchBatchOfEventsSince returns up to limit events with IncrementID greater
	// than lastIncrementID, ordered by IncrementID ascending.
	FetchBatchOfEventsSince(ctx context.Context, lastIncrementID int64, limit int) ([]StoredEvent, error)
}

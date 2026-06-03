package eventstore

import (
	"context"
)

// Store is the minimal write interface for appending domain events.
type Store interface {
	// Append persists one or more domain events to the store.
	Append(ctx context.Context, events ...DomainEvent) error
}

// TransientStore provides head-of-queue access to events that are removed
// after successful processing. It is intended for work-queue style relays
// (see NewTransientRelay) where each event is delivered exactly once and
// then deleted from the store.
type TransientStore interface {
	// FetchBatchOfEvents returns up to limit events starting from the smallest IncrementID.
	FetchBatchOfEvents(ctx context.Context, limit int) ([]StoredEvent, error)
	// CleanUpEvents removes the given events from the store.
	CleanUpEvents(ctx context.Context, events []StoredEvent) error
}

// PointerStore provides cursor-based event fetching.
// Implementations track a position (IncrementID) and return events after it,
// enabling relay consumers to process events exactly once.
type PointerStore interface {
	// FetchBatchOfEventsSince returns up to limit events with IncrementID greater
	// than lastIncrementID, ordered by IncrementID ascending.
	FetchBatchOfEventsSince(ctx context.Context, lastIncrementID int64, limit int) ([]StoredEvent, error)
}

// CleanUpToStore removes all events with IncrementID <= a given threshold
// in a single call. It is intended for outbox cleanup patterns where, once
// a downstream consumer has acknowledged a position, every event at or
// before that position can be discarded.
type CleanUpToStore interface {
	// CleanUpToIncluding removes all events whose IncrementID is less than
	// or equal to incrementID. The event with IncrementID == incrementID,
	// if any, is also removed.
	CleanUpToIncluding(ctx context.Context, incrementID int64) error
}

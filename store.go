package eventstore

import (
	"context"

	"github.com/gofrs/uuid/v5"
)

// Store is the minimal write interface for appending domain events as a plain
// append-only log: events are persisted in the order received with no
// per-stream ordering. For aggregate-style optimistic concurrency, see
// StreamStore.
type Store interface {
	// Append persists the given events in order. Returns an error if marshaling
	// or the underlying write fails.
	Append(ctx context.Context, events ...DomainEvent) error
}

// StreamStore appends events to a single stream with optimistic concurrency
// control, for the event-sourced aggregate pattern (Load → Decide → Save).
type StreamStore interface {
	// AppendWithExpectedVersion atomically verifies that the stream's current
	// head equals expectedVersion and, if so, appends the events at consecutive
	// versions starting at expectedVersion + 1.
	//
	// expectedVersion of -1 requires an empty stream (the create path); N >= 0
	// requires the head to be exactly N.
	//
	// All events MUST have StreamID() == streamID and expectedVersion MUST be
	// >= -1, otherwise the call returns an error wrapping ErrInvalidStreamAppend
	// (a programming error that retry loops must not catch). The check and the
	// insert run in one transaction; a version mismatch is reported as a
	// *StreamVersionConflictError wrapping ErrStreamVersionConflict and rolls
	// back the transaction.
	AppendWithExpectedVersion(
		ctx context.Context,
		streamID uuid.UUID,
		expectedVersion int,
		events ...DomainEvent,
	) error
}

// TransientStore provides head-of-queue access to events that are deleted
// after successful processing, for work-queue style relays
// (NewTransientHandlerRelay / NewTransientBatchHandlerRelay).
//
// Because processing deletes events, a TransientStore must use a dedicated
// table; pointer relays or aggregate streams sharing it would lose data.
// FetchBatchOfEvents does no locking, so competing instances against one store
// cause duplicate processing — scale with WithParallelism instead.
type TransientStore interface {
	// FetchBatchOfEvents returns up to limit events starting from the smallest IncrementID.
	FetchBatchOfEvents(ctx context.Context, limit int) ([]StoredEvent, error)
	// CleanUpEvents removes the given events from the store.
	CleanUpEvents(ctx context.Context, events []StoredEvent) error
}

// PointerStore provides cursor-based event fetching: implementations return
// events after a given IncrementID so relays can process each event once.
type PointerStore interface {
	// FetchBatchOfEventsSince returns up to limit events with IncrementID greater
	// than lastIncrementID, ordered by IncrementID ascending.
	FetchBatchOfEventsSince(ctx context.Context, lastIncrementID int64, limit int) ([]StoredEvent, error)
}

// CleanUpToStore removes all events up to and including a given IncrementID,
// for outbox cleanup once a relay has acknowledged that position.
//
// Deletion is indiscriminate: with several pointer relays the threshold must be
// the minimum cursor across all of them, and the store must not hold aggregate
// streams.
type CleanUpToStore interface {
	// CleanUpToIncluding removes all events with IncrementID <= incrementID.
	CleanUpToIncluding(ctx context.Context, incrementID int64) error
}

// StreamReader reads a single stream, returning events ordered by StreamVersion
// ascending from fromVersion inclusive. Only events written via
// StreamStore.AppendWithExpectedVersion belong to a stream; events from the
// plain Store.Append path are not returned.
type StreamReader interface {
	// ReadStream returns up to limit events for the stream, ordered by
	// StreamVersion ascending, starting at fromVersion (>= 0) inclusive.
	// Because the result is capped at limit, use ReadStreamAll to read a stream
	// of unknown length in full.
	ReadStream(ctx context.Context, streamID uuid.UUID, fromVersion, limit int) ([]StoredEvent, error)
}

// StreamVersionReader returns a stream's current head without loading its
// events — useful as a cheap health check or to obtain an expectedVersion when
// the aggregate was not loaded.
type StreamVersionReader interface {
	// LatestStreamVersion returns the highest StreamVersion persisted for
	// streamID, or -1 if the stream has no versioned events. The -1 sentinel
	// lets callers compute the next version as last + 1.
	LatestStreamVersion(ctx context.Context, streamID uuid.UUID) (int, error)
}

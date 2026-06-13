package eventstore

import (
	"context"

	"github.com/gofrs/uuid/v5"
)

// Store is the minimal write interface for appending domain events.
// Append is a plain "throw it on the log" operation: events are persisted
// in the order received, but the store does not enforce any per-stream
// ordering. Use this for the classic transactional-outbox / projection
// pattern, where you only need durable, ordered event delivery to relays.
//
// For aggregate-style workloads that need optimistic concurrency control
// on a per-stream basis, see StreamStore.
type Store interface {
	// Append persists one or more domain events to the store. The order of
	// events in the slice is preserved on disk. Returns an error if event
	// marshaling fails or the underlying SQL execution fails.
	Append(ctx context.Context, events ...DomainEvent) error
}

// StreamStore is the write interface for appending events to a specific
// stream with an optimistic-concurrency contract. It is the right choice
// for the event-sourced aggregate pattern (Load → Decide → Save), where
// concurrent writers must be detected so the loser can reload and retry.
//
// The expectedVersion argument declares what the caller believes the
// stream's current head to be; the store verifies this atomically with
// the insert. See AppendWithExpectedVersion for the full contract.
type StreamStore interface {
	// AppendWithExpectedVersion atomically appends events to the stream
	// identified by streamID and verifies that the stream's current head
	// equals expectedVersion before any insert happens. If the check
	// passes, the events are persisted with consecutive stream versions
	// starting at expectedVersion + 1.
	//
	// expectedVersion semantics:
	//
	//   - -1: the stream MUST be empty (i.e. this is the first batch of a
	//     fresh aggregate). Useful for "create" paths.
	//   - N ≥ 0: the stream's current head MUST be exactly N. The events
	//     are then persisted at versions N+1, N+2, …. This is the case
	//     after replaying N+1 events during Load.
	//
	// All events in the batch MUST have StreamID() == streamID, and
	// expectedVersion MUST be >= -1; otherwise the call returns an error
	// wrapping ErrInvalidStreamAppend. These are programming errors, not
	// concurrency conflicts — a reload-and-retry loop must not catch them.
	//
	// Concurrency: the check and the insert run inside the same
	// transaction (caller-supplied or store-owned), so concurrent
	// appends to the same stream see either the pre-state or the
	// post-state, never an interleaving. A version mismatch is reported
	// as a *StreamVersionConflictError wrapping ErrStreamVersionConflict
	// and the transaction is rolled back.
	AppendWithExpectedVersion(
		ctx context.Context,
		streamID uuid.UUID,
		expectedVersion int,
		events ...DomainEvent,
	) error
}

// TransientStore provides head-of-queue access to events that are removed
// after successful processing. It is intended for work-queue style relays
// (see NewTransientHandlerRelay / NewTransientBatchHandlerRelay) where
// each event is delivered exactly once and then deleted from the store.
//
// Because processing DELETES events, a TransientStore must not share a
// table with consumers that expect events to be retained: pointer relays
// would silently miss the deleted events, and aggregate streams written
// via StreamStore would lose their history. Use a dedicated table for
// transient workloads.
//
// FetchBatchOfEvents performs no locking, so running multiple instances
// of the same transient relay against one store (competing consumers)
// results in duplicate processing. Scale transient relays by partitioning
// within a single instance (WithParallelism), not by adding instances.
type TransientStore interface {
	// FetchBatchOfEvents returns up to limit events starting from the smallest IncrementID.
	FetchBatchOfEvents(ctx context.Context, limit int) ([]StoredEvent, error)
	// CleanUpEvents removes the given events from the store.
	CleanUpEvents(ctx context.Context, events []StoredEvent) error
}

// PointerStore provides cursor-based event fetching.
// Implementations track a position (IncrementID) and return events after it,
// enabling relays to process events exactly once.
type PointerStore interface {
	// FetchBatchOfEventsSince returns up to limit events with IncrementID greater
	// than lastIncrementID, ordered by IncrementID ascending.
	FetchBatchOfEventsSince(ctx context.Context, lastIncrementID int64, limit int) ([]StoredEvent, error)
}

// CleanUpToStore removes all events with IncrementID <= a given threshold
// in a single call. It is intended for outbox cleanup patterns where, once
// a relay has acknowledged a position, every event at or
// before that position can be discarded.
//
// Deletion is indiscriminate: when several pointer relays read the same
// store, the threshold must be the MINIMUM cursor across all of them, and
// the store must not hold aggregate streams (their history would be
// deleted along with the outbox rows).
type CleanUpToStore interface {
	// CleanUpToIncluding removes all events whose IncrementID is less than
	// or equal to incrementID. The event with IncrementID == incrementID,
	// if any, is also removed.
	CleanUpToIncluding(ctx context.Context, incrementID int64) error
}

// StreamReader is the read interface for a single stream. It returns events
// ordered by StreamVersion ascending, starting at fromVersion *inclusive*
// (a fromVersion of 0 returns the first event of the stream). Streams let
// you reconstruct an aggregate from its events, or build a per-stream
// projection that needs the events of a particular entity in order.
//
// Only events written via StreamStore.AppendWithExpectedVersion are part
// of a stream. Events written via the plain Store.Append path carry no
// StreamVersion and are NOT returned by ReadStream — the two write paths
// are separate worlds that merely share a table.
//
// Implementations are independent of the relay machinery: a Store does not
// have to also implement StreamReader, and vice versa.
type StreamReader interface {
	// ReadStream returns up to limit events for the given stream, ordered
	// by StreamVersion ascending, starting at fromVersion inclusive.
	// fromVersion must be >= 0; fromVersion == 0 returns the event at
	// version 0 (the first event of the stream) and onward. The returned
	// slice may be empty when the stream has no events at or after
	// fromVersion.
	//
	// ReadStream returns AT MOST limit events: a stream longer than limit
	// is truncated, and a caller that reconstructs an aggregate from a
	// truncated history will compute a stale expectedVersion. To read an
	// entire stream regardless of length, use ReadStreamAll, which pages
	// until the stream is exhausted.
	ReadStream(ctx context.Context, streamID uuid.UUID, fromVersion, limit int) ([]StoredEvent, error)
}

// StreamVersionReader returns the per-stream position of a stream
// without loading its events. It serves two purposes:
//
//   - Cheap pre-check: the diagnostic / monitoring / "is the stream
//     healthy?" path that needs the current head without paying for
//     ReadStream.
//   - Source of expectedVersion: a caller that did NOT load the
//     aggregate (e.g. a one-shot appender) can call LatestStreamVersion
//     and pass the result to StreamStore.AppendWithExpectedVersion.
//
// In the typical Load → Decide → Save path, the caller already knows
// the version from the last event it replayed, so LatestStreamVersion
// is not on the hot path — but the interface exists for the cases that
// do need it.
//
// Implementations are independent of StreamReader: a Store is not
// required to satisfy this interface, and not every backend will.
type StreamVersionReader interface {
	// LatestStreamVersion returns the highest StreamVersion persisted for
	// streamID, or -1 if no versioned events exist for that stream
	// (unversioned events written via the plain Store.Append path do not
	// count — see StreamReader). -1 is the sentinel for an empty/fresh
	// stream so callers can compute the next expected version with simple
	// arithmetic:
	//
	//	expected := last + 1
	//
	// works for both a fresh stream (last == -1 → expected == 0) and an
	// existing one (last == N → expected == N+1).
	LatestStreamVersion(ctx context.Context, streamID uuid.UUID) (int, error)
}

package eventstore

import (
	"time"

	"github.com/gofrs/uuid/v5"
)

// Metadata is an optional set of key/value pairs attached to an event for
// cross-cutting concerns (correlation IDs, causation chains, tracing, tenancy,
// user identity, etc.). The map is intentionally map[string]string to keep the
// wire format simple and to prevent schema drift across event types.
//
// Reserved keys (by convention, not enforced by the store):
//
//   - MetadataKeyCorrelationID: links all events triggered by a single
//     incoming request or command, propagated across service boundaries.
//   - MetadataKeyCausationID:   the ID of the event that *caused* this event,
//     i.e. the last event in the chain this one reacts to. Forms a linked
//     list of cause-and-effect when paired with the correlation ID.
//   - MetadataKeyTraceID:       OTel/distributed-tracing trace ID, for log
//     correlation.
//
// Stores MUST treat a nil Metadata and an empty Metadata as equivalent.
type Metadata map[string]string

// Standard reserved keys for cross-cutting metadata. They are not enforced —
// the store persists any keys the producer provides — but consumers are
// encouraged to interpret these names in the conventional way.
const (
	MetadataKeyCorrelationID = "correlation_id"
	MetadataKeyCausationID   = "causation_id"
	MetadataKeyTraceID       = "trace_id"
)

// DomainEvent represents an event that occurred in the domain.
// Implementations carry all data needed to describe what happened
// and are passed to Store.Append to persist the event.
//
// Note: a DomainEvent does NOT carry a per-stream version. The version
// is assigned by the store on append — either implicitly (Store.Append
// treats the event log as an append-only feed) or against an expected
// version supplied to StreamStore.AppendWithExpectedVersion. Aggregates
// that need to know their position on reload get it from the last
// StoredEvent.StreamVersion they replayed.
type DomainEvent interface {
	// ID returns the unique identifier of this event.
	ID() uuid.UUID
	// StreamID returns the identifier of the stream this event belongs to.
	StreamID() uuid.UUID
	// EventType returns a stable string identifier for the event type (e.g. "OrderPlaced").
	EventType() string
	// OccurredAt returns the wall-clock time at which the event occurred.
	OccurredAt() time.Time
	// Metadata returns optional cross-cutting metadata for this event.
	// Implementations should return nil when no metadata is attached; stores
	// MUST treat nil and an empty map as equivalent.
	Metadata() Metadata
}

// StoredEvent is the read model returned by a PointerStore.
// It includes the database-assigned IncrementID which is used by
// relays to track the last processed position.
type StoredEvent struct {
	// IncrementID is the monotonically increasing position assigned by the store.
	IncrementID int64
	// ID is the unique identifier of the event.
	ID uuid.UUID
	// StreamID is the identifier of the stream this event belongs to.
	StreamID uuid.UUID
	// EventType is the stable string identifier for the event type.
	EventType string
	// Payload contains the serialized event data (typically JSON).
	Payload string
	// OccurredAt is the wall-clock time at which the event occurred.
	OccurredAt time.Time
	// Metadata carries the cross-cutting metadata persisted alongside the event.
	// It is nil for events that did not have metadata when they were appended.
	Metadata Metadata
	// StreamVersion is the per-stream position this event occupies. Stores
	// enforce that, for each stream, this equals the previously-stored
	// StreamVersion + 1, or 0 for the first event of a fresh stream.
	StreamVersion int
}

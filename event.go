package eventstore

import (
	"time"

	"github.com/gofrs/uuid/v5"
)

// Metadata is an optional set of key/value pairs attached to an event for
// cross-cutting concerns (correlation IDs, causation chains, tracing). Stores
// treat a nil and an empty Metadata as equivalent.
type Metadata map[string]string

// Reserved metadata keys for cross-cutting concerns. They are conventions only;
// the store persists whatever keys the producer provides.
const (
	MetadataKeyCorrelationID = "correlation_id"
	MetadataKeyCausationID   = "causation_id"
	MetadataKeyTraceID       = "trace_id"
)

// DomainEvent represents an event that occurred in the domain. Implementations
// are passed to Store.Append to be persisted. A DomainEvent carries no stream
// version of its own; the store assigns one on append.
type DomainEvent interface {
	// ID returns the unique identifier of this event.
	ID() uuid.UUID
	// StreamID returns the identifier of the stream this event belongs to.
	StreamID() uuid.UUID
	// EventType returns a stable string identifier for the event type (e.g. "OrderPlaced").
	EventType() string
	// OccurredAt returns the wall-clock time at which the event occurred.
	OccurredAt() time.Time
}

// MetadataProvider is an optional interface a DomainEvent can implement to
// attach cross-cutting metadata. Events that do not implement it are persisted
// with nil metadata.
type MetadataProvider interface {
	// Metadata returns the event's metadata, or nil when none is attached.
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
	// StreamVersion is the per-stream position assigned by
	// StreamStore.AppendWithExpectedVersion. Events written via the plain
	// Store.Append path are unversioned and delivered with StreamVersion == -1.
	StreamVersion int
}

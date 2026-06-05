package eventstore

import (
	"time"

	"github.com/gofrs/uuid/v5"
)

// DomainEvent represents an event that occurred in the domain.
// Implementations carry all data needed to describe what happened
// and are passed to Store.Append to persist the event.
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
}

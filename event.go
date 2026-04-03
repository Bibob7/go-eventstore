package eventstore

import (
	"time"

	"github.com/gofrs/uuid/v5"
)

type DomainEvent interface {
	ID() uuid.UUID
	AggregateID() uuid.UUID
	EventType() string
	OccurredAt() time.Time
}

type StoredEvent struct {
	IncrementID int64
	ID          uuid.UUID
	EntityID    uuid.UUID
	EventType   string
	Payload     string
	OccurredAt  time.Time
}

package mysql

import (
	"database/sql"
)

// EventStoreBundle groups the event store and increment ID store together for convenience.
type EventStoreBundle struct {
	EventStore       *EventStore
	IncrementIDStore *EventIncrementIDStore
}

// NewEventStoreBundle creates a MySQL connection and initializes event store infrastructure.
func NewEventStoreBundle(db *sql.DB, cfg Config) (*EventStoreBundle, error) {
	return &EventStoreBundle{
		EventStore:       NewEventStore(db, cfg.OutboxTableName),
		IncrementIDStore: NewEventIncrementIDStore(db, cfg.IncrementIDTableName),
	}, nil
}

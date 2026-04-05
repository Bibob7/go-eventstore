package mysql

import (
	"database/sql"
)

// EventStoreBundle groups the event store and increment ID store together for convenience.
type EventStoreBundle struct {
	EventStore       *EventStore
	IncrementIDStore *EventIncrementIDStore
}

// NewEventStoreBundle initializes a new EventStoreBundle with the given database connection and configuration.
func NewEventStoreBundle(db *sql.DB, cfg Config) *EventStoreBundle {
	return &EventStoreBundle{
		EventStore:       NewEventStore(db, cfg.OutboxTableName),
		IncrementIDStore: NewEventIncrementIDStore(db, cfg.IncrementIDTableName),
	}
}

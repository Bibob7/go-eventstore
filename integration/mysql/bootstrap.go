package mysql

import (
	"database/sql"
)

// EventStoreBundle groups the event store and increment ID store for convenience.
type EventStoreBundle struct {
	EventStore       *EventStore
	IncrementIDStore *EventIncrementIDStore
}

// NewEventStoreBundle builds an EventStoreBundle from db and cfg.
func NewEventStoreBundle(db *sql.DB, cfg Config) *EventStoreBundle {
	return &EventStoreBundle{
		EventStore:       NewEventStore(db, cfg.EventStoreTableName),
		IncrementIDStore: NewEventIncrementIDStore(db, cfg.IncrementIDTableName),
	}
}

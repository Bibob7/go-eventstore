package mysql

import (
	"database/sql"
	"github.com/Bibob7/go-eventstore"
	"io"
)

// EventStoreBundle groups a MySQL DB connection with its event store infrastructure.
type EventStoreBundle struct {
	DB               *sql.DB
	EventStore       *EventStore
	IncrementIDStore eventstore.IncrementIDStore
}

// NewEventStoreBundle creates a MySQL connection and initializes event store infrastructure.
func NewEventStoreBundle(db *sql.DB, cfg eventstore.Config) (*EventStoreBundle, error) {
	return &EventStoreBundle{
		DB:               db,
		EventStore:       NewEventStore(db, cfg.OutboxTableName),
		IncrementIDStore: NewEventIncrementIDStore(db, cfg.IncrementIDTableName),
	}, nil
}

// Closer returns the DB connection as an io.Closer for use with ClosableModule.Closables.
func (b *EventStoreBundle) Closer() io.Closer { return b.DB }

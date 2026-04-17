package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type EventIncrementIDStore struct {
	db        *sql.DB
	tableName string
}

// NewEventIncrementIDStore constructs an EventIncrementIDStore bound to the
// given database and table. The table name must be a valid SQL identifier;
// otherwise this function panics.
func NewEventIncrementIDStore(db *sql.DB, tableName string) *EventIncrementIDStore {
	mustValidateIdentifier("tableName", tableName)
	return &EventIncrementIDStore{
		db:        db,
		tableName: tableName,
	}
}

func (s *EventIncrementIDStore) SetIncrementID(ctx context.Context, consumerName string, incrementID int64) error {
	// #nosec G201
	stmt := fmt.Sprintf("INSERT INTO %s (consumer_name, increment_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE increment_id = VALUES(increment_id)", s.tableName)
	_, err := s.db.ExecContext(ctx, stmt, consumerName, incrementID)
	return err
}

func (s *EventIncrementIDStore) GetIncrementID(ctx context.Context, consumerName string) (int64, error) {
	// #nosec G201
	stmt := fmt.Sprintf("SELECT increment_id FROM %s WHERE consumer_name = ?", s.tableName)
	var incrementID int64
	err := s.db.QueryRowContext(ctx, stmt, consumerName).Scan(&incrementID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return incrementID, err
}

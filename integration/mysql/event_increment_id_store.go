package mysql

import (
	"context"
	"database/sql"
	"errors"
	"eventstore"
)

type eventIncrementIDStore struct {
	db *sql.DB
}

func NewEventIncrementIDStore(db *sql.DB) eventstore.IncrementIDStore {
	return eventIncrementIDStore{
		db: db,
	}
}

func (s eventIncrementIDStore) SetIncrementID(ctx context.Context, consumerName string, incrementID int64) error {
	stmt := "INSERT INTO event_increment_id (consumer_name, increment_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE increment_id = VALUES(increment_id)"
	_, err := s.db.ExecContext(ctx, stmt, consumerName, incrementID)
	return err
}

func (s eventIncrementIDStore) GetIncrementID(ctx context.Context, consumerName string) (int64, error) {
	stmt := "SELECT increment_id FROM event_increment_id WHERE consumer_name = ?"
	var incrementID int64
	err := s.db.QueryRowContext(ctx, stmt, consumerName).Scan(&incrementID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return incrementID, err
}

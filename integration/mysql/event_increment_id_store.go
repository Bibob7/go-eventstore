package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/Bibob7/go-eventstore"
)

type eventIncrementIDStore struct {
	db        *sql.DB
	tableName string
}

func NewEventIncrementIDStore(db *sql.DB, tableName string) eventstore.IncrementIDStore {
	return eventIncrementIDStore{
		db:        db,
		tableName: tableName,
	}
}

func (s eventIncrementIDStore) SetIncrementID(ctx context.Context, consumerName string, incrementID int64) error {
	// #nosec G201
	stmt := fmt.Sprintf("INSERT INTO %s (consumer_name, increment_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE increment_id = VALUES(increment_id)", s.tableName)
	_, err := s.db.ExecContext(ctx, stmt, consumerName, incrementID)
	return err
}

func (s eventIncrementIDStore) GetIncrementID(ctx context.Context, consumerName string) (int64, error) {
	// #nosec G201
	stmt := fmt.Sprintf("SELECT increment_id FROM %s WHERE consumer_name = ?", s.tableName)
	var incrementID int64
	err := s.db.QueryRowContext(ctx, stmt, consumerName).Scan(&incrementID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return incrementID, err
}

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/Bibob7/go-eventstore"
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

func (s *EventIncrementIDStore) SetIncrementID(ctx context.Context, consumerName string, expectedPreviousID int64, incrementID int64) error {
	// #nosec G201
	updateStmt := fmt.Sprintf("UPDATE %s SET increment_id = ? WHERE consumer_name = ? AND increment_id = ?", s.tableName)
	result, err := s.db.ExecContext(ctx, updateStmt, incrementID, consumerName, expectedPreviousID)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 1 {
		return nil
	}

	if expectedPreviousID == 0 {
		// #nosec G201
		insertStmt := fmt.Sprintf("INSERT INTO %s (consumer_name, increment_id) VALUES (?, ?)", s.tableName)
		_, err = s.db.ExecContext(ctx, insertStmt, consumerName, incrementID)
		if err == nil {
			return nil
		}
	}

	currentIncrementID, getErr := s.GetIncrementID(ctx, consumerName)
	if getErr != nil {
		return getErr
	}
	if currentIncrementID != expectedPreviousID {
		return eventstore.ErrIncrementIDConflict
	}
	if currentIncrementID == incrementID {
		return nil
	}

	return eventstore.ErrIncrementIDConflict
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

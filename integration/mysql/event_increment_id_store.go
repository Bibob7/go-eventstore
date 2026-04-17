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
	return WithTransaction(ctx, s.db, func(tx *sql.Tx) error {
		// #nosec G201
		selectStmt := fmt.Sprintf("SELECT increment_id FROM %s WHERE consumer_name = ? FOR UPDATE", s.tableName)
		var currentID int64
		err := tx.QueryRowContext(ctx, selectStmt, consumerName).Scan(&currentID)

		if errors.Is(err, sql.ErrNoRows) {
			if expectedPreviousID != 0 {
				return eventstore.ErrIncrementIDConflict
			}
			// #nosec G201
			insertStmt := fmt.Sprintf("INSERT INTO %s (consumer_name, increment_id) VALUES (?, ?)", s.tableName)
			if _, err = tx.ExecContext(ctx, insertStmt, consumerName, incrementID); err != nil {
				return err
			}
			return nil
		}
		if err != nil {
			return err
		}

		if currentID != expectedPreviousID {
			return eventstore.ErrIncrementIDConflict
		}

		if currentID != incrementID {
			// #nosec G201
			updateStmt := fmt.Sprintf("UPDATE %s SET increment_id = ? WHERE consumer_name = ?", s.tableName)
			if _, err = tx.ExecContext(ctx, updateStmt, incrementID, consumerName); err != nil {
				return err
			}
		}
		return nil
	}, nil)
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

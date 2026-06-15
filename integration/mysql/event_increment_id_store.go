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

func (s *EventIncrementIDStore) SetIncrementID(ctx context.Context, relayName string, expectedPreviousID int64, incrementID int64) error {
	return WithTransaction(ctx, s.db, func(tx *sql.Tx) error {
		// Ensure the row exists before the FOR UPDATE below: a FOR UPDATE on a
		// missing row takes a gap lock that deadlocks when several relays insert
		// distinct cursors concurrently. The upsert takes only insert-intention
		// locks, so the FOR UPDATE then locks an existing row.
		// #nosec G201
		ensureStmt := fmt.Sprintf("INSERT INTO %s (relay_name, increment_id) VALUES (?, 0) ON DUPLICATE KEY UPDATE relay_name = relay_name", s.tableName)
		if _, err := tx.ExecContext(ctx, ensureStmt, relayName); err != nil {
			return err
		}

		// #nosec G201
		selectStmt := fmt.Sprintf("SELECT increment_id FROM %s WHERE relay_name = ? FOR UPDATE", s.tableName)
		var currentID int64
		if err := tx.QueryRowContext(ctx, selectStmt, relayName).Scan(&currentID); err != nil {
			return err
		}

		if currentID != expectedPreviousID {
			return eventstore.ErrIncrementIDConflict
		}

		if currentID != incrementID {
			// #nosec G201
			updateStmt := fmt.Sprintf("UPDATE %s SET increment_id = ? WHERE relay_name = ?", s.tableName)
			if _, err := tx.ExecContext(ctx, updateStmt, incrementID, relayName); err != nil {
				return err
			}
		}
		return nil
	}, nil)
}

func (s *EventIncrementIDStore) GetIncrementID(ctx context.Context, relayName string) (int64, error) {
	// #nosec G201
	stmt := fmt.Sprintf("SELECT increment_id FROM %s WHERE relay_name = ?", s.tableName)
	var incrementID int64
	err := s.db.QueryRowContext(ctx, stmt, relayName).Scan(&incrementID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return incrementID, err
}

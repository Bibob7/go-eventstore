package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"github.com/Bibob7/go-eventstore"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-sql-driver/mysql"
)

const (
	statePending = "pending"
	stateSuccess = "success"
	stateFailed  = "failed"
)

type idempotencyRegistry struct {
	db        *sql.DB
	tableName string
}

// NewIdempotencyRegistry creates a new MySQL-backed sendout registry.
func NewIdempotencyRegistry(db *sql.DB, tableName string) *idempotencyRegistry {
	return &idempotencyRegistry{
		db:        db,
		tableName: tableName,
	}
}

// RegisterKey registers or updates an entry for the given idempotency key with state "pending".
// Behavior per key:
// - If no row exists: insert with state "pending".
// - If state is "failed": update to "pending" (retry allowed).
// - If state is "pending" or "success": return eventstore.ErrAlreadyExist.
// The operation is executed within a transaction to ensure consistency.
func (s idempotencyRegistry) RegisterKey(ctx context.Context, key, namespace string) error {
	if key == "" || namespace == "" {
		return fmt.Errorf("key and namespace must not be empty")
	}
	hashed := hashKey(key, namespace)

	const maxRetry = 3
	var attempt int

	for {
		attempt++
		err := s.registerKeyOnce(ctx, hashed)
		if err == nil {
			return nil
		}

		if errors.Is(err, eventstore.ErrAlreadyExist) {
			return err
		}

		var retryable bool
		if mysqlErr, ok := errors.AsType[*mysql.MySQLError](err); ok {
			if mysqlErr.Number == 1213 {
				retryable = true
			}
		}

		if attempt < maxRetry && retryable {
			slog.Warn("mysql retryable error detected", "key", key, "err", err)
			time.Sleep(time.Duration(attempt*20) * time.Millisecond)
			continue
		}
		return err
	}
}

func (s idempotencyRegistry) registerKeyOnce(ctx context.Context, key []byte) error {
	return WithTransaction(ctx, s.db, func(tx *sql.Tx) error {
		// #nosec G201
		qUpsert := fmt.Sprintf(
			"INSERT INTO %s (idempotency_key, state, created_at, updated_at) VALUES (?, ?, NOW(), NOW()) "+
				"ON DUPLICATE KEY UPDATE state = IF(state = ?, VALUES(state), state), updated_at = NOW()",
			s.tableName,
		)
		if _, err := tx.ExecContext(ctx, qUpsert, key, statePending, stateFailed); err != nil {
			return fmt.Errorf("upsert pending: %w", err)
		}

		var curr string
		// #nosec G201
		qSelect := fmt.Sprintf("SELECT state FROM %s WHERE idempotency_key = ?", s.tableName)
		if err := tx.QueryRowContext(ctx, qSelect, key).Scan(&curr); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("state not found after upsert")
			}
			return fmt.Errorf("select state after upsert: %w", err)
		}

		switch curr {
		case statePending:
			return nil
		case stateSuccess:
			return eventstore.ErrAlreadyExist
		default:
			return nil
		}
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}

// MarkAsSuccess sets state to "success" for the given idempotency key.
// Returns an error if a target row does not exist.
func (s idempotencyRegistry) MarkAsSuccess(ctx context.Context, key, namespace string) error {
	if key == "" || namespace == "" {
		return fmt.Errorf("key and namespace must not be empty")
	}
	hashed := hashKey(key, namespace)

	return WithTransaction(ctx, s.db, func(tx *sql.Tx) error {
		// #nosec G201
		q := fmt.Sprintf("UPDATE %s SET state = ?, updated_at = NOW() WHERE idempotency_key = ?", s.tableName)
		res, execErr := tx.ExecContext(ctx, q, stateSuccess, hashed)
		if execErr != nil {
			return fmt.Errorf("update to success: %w", execErr)
		}
		affected, affErr := res.RowsAffected()
		if affErr != nil {
			return fmt.Errorf("rows affected (success): %w", affErr)
		}
		if affected == 0 {
			return fmt.Errorf("entry not found for key %s", key)
		}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
}

func hashKey(payload, namespace string) []byte {
	sum := sha256.Sum256([]byte(payload + namespace))
	return sum[:]
}

// MarkAsFailed sets state to "failed" for the given idempotency key.
// Returns an error if a target row does not exist.
func (s idempotencyRegistry) MarkAsFailed(ctx context.Context, key, namespace string) error {
	if key == "" || namespace == "" {
		return fmt.Errorf("key and namespace must not be empty")
	}
	hashed := hashKey(key, namespace)

	return WithTransaction(ctx, s.db, func(tx *sql.Tx) error {
		// #nosec G201
		q := fmt.Sprintf("UPDATE %s SET state = ?, updated_at = NOW() WHERE idempotency_key = ?", s.tableName)
		res, execErr := tx.ExecContext(ctx, q, stateFailed, hashed)
		if execErr != nil {
			return fmt.Errorf("update to failed: %w", execErr)
		}
		affected, affErr := res.RowsAffected()
		if affErr != nil {
			return fmt.Errorf("rows affected (failed): %w", affErr)
		}
		if affected == 0 {
			return fmt.Errorf("entry not found for key %s", key)
		}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
}

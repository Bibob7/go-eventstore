package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Bibob7/go-eventstore"
)

const incrementIDTable = "event_increment_id"

func ensureIncrementIDTable(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stmt := `CREATE TABLE IF NOT EXISTS event_increment_id (
        consumer_name VARCHAR(255) NOT NULL,
        increment_id BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (consumer_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`

	_, err := db.ExecContext(ctx, stmt)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, "TRUNCATE TABLE "+incrementIDTable)
	require.NoError(t, err)
}

func TestEventIncrementIDStore_GetIncrementID_EmptyReturnsZero(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()
	ensureIncrementIDTable(t, db)

	store := NewEventIncrementIDStore(db, incrementIDTable)
	incrementID, err := store.GetIncrementID(context.Background(), "relay-a")

	require.NoError(t, err)
	require.Equal(t, int64(0), incrementID)
}

func TestEventIncrementIDStore_SetIncrementID(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()
	ensureIncrementIDTable(t, db)

	store := NewEventIncrementIDStore(db, incrementIDTable)
	ctx := context.Background()

	err := store.SetIncrementID(ctx, "relay-a", 0, 10)
	require.NoError(t, err)

	incrementID, err := store.GetIncrementID(ctx, "relay-a")
	require.NoError(t, err)
	require.Equal(t, int64(10), incrementID)

	err = store.SetIncrementID(ctx, "relay-a", 10, 11)
	require.NoError(t, err)

	incrementID, err = store.GetIncrementID(ctx, "relay-a")
	require.NoError(t, err)
	require.Equal(t, int64(11), incrementID)
}

func TestEventIncrementIDStore_SetIncrementID_Conflict(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()
	ensureIncrementIDTable(t, db)

	store := NewEventIncrementIDStore(db, incrementIDTable)
	ctx := context.Background()

	err := store.SetIncrementID(ctx, "relay-a", 0, 10)
	require.NoError(t, err)

	err = store.SetIncrementID(ctx, "relay-a", 9, 11)
	require.Error(t, err)
	require.True(t, errors.Is(err, eventstore.ErrIncrementIDConflict))

	incrementID, getErr := store.GetIncrementID(ctx, "relay-a")
	require.NoError(t, getErr)
	require.Equal(t, int64(10), incrementID)
}

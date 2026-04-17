package mysql

import (
	"context"
	"database/sql"
	"errors"
	"sync"
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

func TestEventIncrementIDStore_SetIncrementID_NoOp(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()
	ensureIncrementIDTable(t, db)

	store := NewEventIncrementIDStore(db, incrementIDTable)
	ctx := context.Background()

	err := store.SetIncrementID(ctx, "relay-a", 0, 10)
	require.NoError(t, err)

	err = store.SetIncrementID(ctx, "relay-a", 10, 10)
	require.NoError(t, err)

	incrementID, getErr := store.GetIncrementID(ctx, "relay-a")
	require.NoError(t, getErr)
	require.Equal(t, int64(10), incrementID)
}

func TestEventIncrementIDStore_SetIncrementID_MissingRowWithNonZeroExpectedConflict(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()
	ensureIncrementIDTable(t, db)

	store := NewEventIncrementIDStore(db, incrementIDTable)

	err := store.SetIncrementID(context.Background(), "relay-a", 10, 11)
	require.Error(t, err)
	require.True(t, errors.Is(err, eventstore.ErrIncrementIDConflict))

	incrementID, getErr := store.GetIncrementID(context.Background(), "relay-a")
	require.NoError(t, getErr)
	require.Equal(t, int64(0), incrementID)
}

func TestEventIncrementIDStore_SetIncrementID_ConcurrentConflict(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()
	ensureIncrementIDTable(t, db)

	store := NewEventIncrementIDStore(db, incrementIDTable)
	ctx := context.Background()

	err := store.SetIncrementID(ctx, "relay-a", 0, 10)
	require.NoError(t, err)

	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup

	runWriter := func(nextID int64) {
		defer wg.Done()
		<-start
		results <- store.SetIncrementID(ctx, "relay-a", 10, nextID)
	}

	wg.Add(2)
	go runWriter(11)
	go runWriter(12)

	close(start)
	wg.Wait()
	close(results)

	var errs []error
	for err := range results {
		errs = append(errs, err)
	}

	require.Len(t, errs, 2)

	successes := 0
	conflicts := 0
	for _, err := range errs {
		if err == nil {
			successes++
			continue
		}
		require.True(t, errors.Is(err, eventstore.ErrIncrementIDConflict))
		conflicts++
	}

	require.Equal(t, 1, successes)
	require.Equal(t, 1, conflicts)

	incrementID, getErr := store.GetIncrementID(ctx, "relay-a")
	require.NoError(t, getErr)
	require.Contains(t, []int64{11, 12}, incrementID)
}

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

	store := NewEventIncrementIDStore(db, incrementIDTable)

	type seedStep struct {
		expectedPreviousID int64
		incrementID        int64
	}

	tests := []struct {
		name               string
		consumerName       string
		seed               []seedStep
		expectedPreviousID int64
		incrementID        int64
		wantErr            error
		wantStoredID       int64
	}{
		{
			name:               "first insert with expectedPreviousID=0",
			consumerName:       "relay-a",
			expectedPreviousID: 0,
			incrementID:        10,
			wantStoredID:       10,
		},
		{
			name:               "update with correct expectedPreviousID",
			consumerName:       "relay-a",
			seed:               []seedStep{{0, 10}},
			expectedPreviousID: 10,
			incrementID:        11,
			wantStoredID:       11,
		},
		{
			name:               "conflict on wrong expectedPreviousID",
			consumerName:       "relay-a",
			seed:               []seedStep{{0, 10}, {10, 11}},
			expectedPreviousID: 9,
			incrementID:        12,
			wantErr:            eventstore.ErrIncrementIDConflict,
			wantStoredID:       11,
		},
		{
			name:               "idempotent no-op when expectedPreviousID equals incrementID",
			consumerName:       "relay-a",
			seed:               []seedStep{{0, 10}, {10, 11}},
			expectedPreviousID: 11,
			incrementID:        11,
			wantStoredID:       11,
		},
		{
			name:               "conflict when row missing but expectedPreviousID non-zero",
			consumerName:       "relay-missing",
			expectedPreviousID: 10,
			incrementID:        11,
			wantErr:            eventstore.ErrIncrementIDConflict,
			wantStoredID:       0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ensureIncrementIDTable(t, db)

			for _, s := range tt.seed {
				require.NoError(t, store.SetIncrementID(context.Background(), tt.consumerName, s.expectedPreviousID, s.incrementID))
			}

			err := store.SetIncrementID(context.Background(), tt.consumerName, tt.expectedPreviousID, tt.incrementID)

			if tt.wantErr != nil {
				require.Error(t, err)
				require.True(t, errors.Is(err, tt.wantErr))
			} else {
				require.NoError(t, err)
			}

			incrementID, getErr := store.GetIncrementID(context.Background(), tt.consumerName)
			require.NoError(t, getErr)
			require.Equal(t, tt.wantStoredID, incrementID)
		})
	}
}

func TestEventIncrementIDStore_SetIncrementID_Concurrent(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()

	store := NewEventIncrementIDStore(db, incrementIDTable)

	tests := []struct {
		name               string
		seedIncrementID    int64
		expectedPreviousID int64
		writerIDs          []int64
		wantPossibleIDs    []int64
	}{
		{
			name:               "concurrent updates with same expectedPreviousID",
			seedIncrementID:    10,
			expectedPreviousID: 10,
			writerIDs:          []int64{11, 12},
			wantPossibleIDs:    []int64{11, 12},
		},
		{
			name:               "concurrent inserts on empty row",
			seedIncrementID:    0,
			expectedPreviousID: 0,
			writerIDs:          []int64{10, 20},
			wantPossibleIDs:    []int64{10, 20},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ensureIncrementIDTable(t, db)
			ctx := context.Background()

			if tt.seedIncrementID != 0 {
				require.NoError(t, store.SetIncrementID(ctx, "relay-a", 0, tt.seedIncrementID))
			}

			start := make(chan struct{})
			results := make(chan error, len(tt.writerIDs))
			var wg sync.WaitGroup

			for _, id := range tt.writerIDs {
				wg.Add(1)
				go func(nextID int64) {
					defer wg.Done()
					<-start
					results <- store.SetIncrementID(ctx, "relay-a", tt.expectedPreviousID, nextID)
				}(id)
			}

			close(start)
			wg.Wait()
			close(results)

			var errs []error
			for err := range results {
				errs = append(errs, err)
			}

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
			require.Contains(t, tt.wantPossibleIDs, incrementID)
		})
	}
}

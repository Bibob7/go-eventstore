package mysql

import (
	"context"
	"database/sql"
	"eventstore"
	"eventstore/filter"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gofrs/uuid/v5"
	"github.com/stretchr/testify/require"
)

const outboxTable = "outbox"

func getenvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func testDSN() string {
	host := getenvDefault("MYSQL_HOST", "mysql-test")
	port := getenvDefault("MYSQL_PORT", "3306")
	user := getenvDefault("MYSQL_USER", "test")
	pass := getenvDefault("MYSQL_PASSWORD", "test")
	database := getenvDefault("MYSQL_DATABASE", "su-photography")
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, pass, host, port, database)
}

// ensureOutboxTable makes sure the outbox table exists with the expected schema (compatible with our EventStore)
func ensureOutboxTable(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create table if not exists. Schema mirrored from db/wedding/migrations/000008_add_outbox_table.up.sql
	stmt := `CREATE TABLE IF NOT EXISTS outbox (
        id INT NOT NULL AUTO_INCREMENT,
        event_id BINARY(16) NOT NULL,
        aggregate_id BINARY(16) NOT NULL,
        event_type VARCHAR(255) NOT NULL,
        payload JSON NOT NULL,
        occurred_at DATETIME NOT NULL,
        PRIMARY KEY (id),
        KEY aggregate_id_idx (aggregate_id),
        KEY event_type_idx (event_type),
        KEY event_id_idx (event_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`

	_, err := db.ExecContext(ctx, stmt)
	require.NoError(t, err)

	// Clean slate for each test run
	_, err = db.ExecContext(ctx, "TRUNCATE TABLE "+outboxTable)
	require.NoError(t, err)
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	// Integration tests are opt-in to keep the default test run fast and hermetic.
	// Set INTEGRATION_TESTS=1 (and preferably run docker-compose-test.yml) to enable.
	if os.Getenv("INTEGRATION_TESTS") != "1" {
		t.Skip("MySQL integration tests are disabled by default. Set INTEGRATION_TESTS=1 and start the DB from docker-compose-test.yml to run them.")
	}
	db, err := sql.Open("mysql", testDSN())
	require.NoError(t, err)

	// Wait until DB is ready
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		err = db.PingContext(ctx)
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("database not ready: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	return db
}

func insertEvent(t *testing.T, exec interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, id int64, eventType string) {
	t.Helper()
	evtID, _ := uuid.NewV4()
	aggID, _ := uuid.NewV4()
	payload := "{}"
	occurredAt := time.Now().Format(time.DateTime)

	// Explicitly set id to craft gaps
	stmt := fmt.Sprintf("INSERT INTO %s (id, event_id, aggregate_id, event_type, payload, occurred_at) VALUES (?, ?, ?, ?, ?, ?)", outboxTable)
	_, err := exec.ExecContext(context.Background(), stmt, id, mustBinary(evtID), mustBinary(aggID), eventType, payload, occurredAt)
	require.NoError(t, err)
}

func mustBinary(u uuid.UUID) []byte {
	b, _ := u.MarshalBinary()
	return b
}

func fetchIDs(events []eventstore.StoredEvent) []int64 {
	ids := make([]int64, len(events))
	for i, e := range events {
		ids[i] = e.IncrementID
	}
	return ids
}

func TestEventStore_GapDetection_NoGaps(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ensureOutboxTable(t, db)

	// Insert committed contiguous events: 1,2,3
	insertEvent(t, db, 1, "test")
	insertEvent(t, db, 2, "test")
	insertEvent(t, db, 3, "test")

	store := NewEventStore(db, outboxTable)
	ctx := context.Background()

	events, err := store.FetchBatchOfEventsSince(ctx, -1, 10)
	require.NoError(t, err)
	require.Len(t, events, 3)
	require.Equal(t, []int64{1, 2, 3}, fetchIDs(events))
}

func TestEventStore_GapDetection_GapWithUncommittedPresent(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ensureOutboxTable(t, db)

	// Committed: 1,2,5
	insertEvent(t, db, 1, "test")
	insertEvent(t, db, 2, "test")
	insertEvent(t, db, 5, "test")

	// Open a separate transaction and insert ID 3 but do not commit yet
	tx, err := db.Begin()
	require.NoError(t, err)
	insertEvent(t, tx, 3, "test")
	// Note: we intentionally do not commit until after the fetch
	defer tx.Rollback()

	store := NewEventStore(db, outboxTable)
	ctx := context.Background()

	events, err := store.FetchBatchOfEventsSince(ctx, 0, 10)
	require.NoError(t, err)

	// We expect the filter to stop before the gap because ID 3 exists (uncommitted) → only 1,2 returned
	require.Equal(t, []int64{1, 2}, fetchIDs(events))
}

func TestEventStore_GapDetection_GapWithMultipleUncommittedPresent(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ensureOutboxTable(t, db)

	// Committed: 1,2,5
	insertEvent(t, db, 1, "test")
	insertEvent(t, db, 2, "test")
	insertEvent(t, db, 12, "test")

	// Open a separate transaction and insert ID 3 but do not commit yet
	tx, err := db.Begin()
	require.NoError(t, err)
	insertEvent(t, tx, 3, "test")
	insertEvent(t, tx, 4, "test")
	insertEvent(t, tx, 5, "test")
	insertEvent(t, tx, 6, "test")
	insertEvent(t, tx, 7, "test")
	insertEvent(t, tx, 10, "test")
	// Note: we intentionally do not commit until after the fetch
	defer tx.Rollback()

	store := NewEventStore(db, outboxTable)
	ctx := context.Background()

	events, err := store.FetchBatchOfEventsSince(ctx, 0, 10)
	require.NoError(t, err)

	// We expect the filter to stop before the gap because ID 3 exists (uncommitted) → only 1,2 returned
	require.Equal(t, []int64{1, 2}, fetchIDs(events))
}

func TestEventStore_GapDetection_GapWithRepeatableAttempt(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ensureOutboxTable(t, db)

	// Committed: 1,2,5
	insertEvent(t, db, 1, "test")
	insertEvent(t, db, 2, "test")
	insertEvent(t, db, 12, "test")

	// Open a separate transaction and insert ID 3 but do not commit yet
	tx, err := db.Begin()
	require.NoError(t, err)
	insertEvent(t, tx, 3, "test")
	insertEvent(t, tx, 4, "test")
	insertEvent(t, tx, 5, "test")
	insertEvent(t, tx, 6, "test")
	insertEvent(t, tx, 7, "test")
	insertEvent(t, tx, 10, "test")

	store := NewEventStore(db, outboxTable)
	ctx := context.Background()

	firstEvents, err := store.FetchBatchOfEventsSince(ctx, 0, 10)
	require.NoError(t, err)
	// Now commit the transaction to make those events visible
	err = tx.Commit()
	require.NoError(t, err)
	secondEvents, err := store.FetchBatchOfEventsSince(ctx, 0, 10)
	require.NoError(t, err)

	// We expect the filter to stop before the gap because ID 3 exists (uncommitted) → only 1,2 returned
	require.Equal(t, []int64{1, 2}, fetchIDs(firstEvents))
	// After commit, we expect all events to be returned
	require.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 10, 12}, fetchIDs(secondEvents))
}

func TestEventStore_GapDetection_GapWithoutUncommittedPresent(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ensureOutboxTable(t, db)

	// Committed: 1,2,5 (no uncommitted rows)
	insertEvent(t, db, 1, "test")
	insertEvent(t, db, 2, "test")
	insertEvent(t, db, 5000, "test")

	store := NewEventStore(db, outboxTable)
	ctx := context.Background()

	events, err := store.FetchBatchOfEventsSince(ctx, 0, 10)
	require.NoError(t, err)

	// Since missing IDs (3,4) are not present (even uncommitted), the filter continues and includes 5
	require.Equal(t, []int64{1, 2, 5000}, fetchIDs(events))
}

func TestEventStore_GapDetection_HugeGapWithoutUncommittedPresent(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ensureOutboxTable(t, db)

	// Committed: 1,2,5 (no uncommitted rows)
	insertEvent(t, db, 1, "test")
	insertEvent(t, db, 2, "test")
	insertEvent(t, db, 5, "test")

	store := NewEventStore(db, outboxTable)
	ctx := context.Background()

	events, err := store.FetchBatchOfEventsSince(ctx, 0, 10)
	require.NoError(t, err)

	// Since missing IDs (3,4) are not present (even uncommitted), the filter continues and includes 5
	require.Equal(t, []int64{1, 2, 5}, fetchIDs(events))
}

func TestEventStore_GapDetection_SinceLastIncrementID(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ensureOutboxTable(t, db)

	// Committed: 3,4,6 (start since 2)
	insertEvent(t, db, 3, "test")
	insertEvent(t, db, 4, "test")
	insertEvent(t, db, 6, "test")

	store := NewEventStore(db, outboxTable)
	ctx := context.Background()

	events, err := store.FetchBatchOfEventsSince(ctx, 2, 10)
	require.NoError(t, err)

	// Missing 5 but not present in DB → continue and include 6
	require.Equal(t, []int64{3, 4, 6}, fetchIDs(events))
}

// countingGapDetector is a GapDetector that counts how often HasUncommittedID is called.
type countingGapDetector struct {
	store *EventStore
	calls int
}

func (c *countingGapDetector) HasUncommittedID(ctx context.Context, low, high int64) (bool, error) {
	c.calls++
	return c.store.HasUncommittedID(ctx, low, high)
}

// Test ensures that HasUncommittedID is called exactly once for a large gap.
func TestEventStore_GapDetection_HasUncommittedCalledOnceForLargeGap(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ensureOutboxTable(t, db)

	insertEvent(t, db, 1, "test")
	insertEvent(t, db, 2, "test")
	insertEvent(t, db, 10, "test")

	store := NewEventStore(db, outboxTable)
	det := &countingGapDetector{store: store}

	input := []eventstore.StoredEvent{
		{IncrementID: 1},
		{IncrementID: 2},
		{IncrementID: 10},
	}

	ctx := context.Background()
	filtered, err := filter.NewUntilGapEventFilter(0, det).Execute(ctx, input)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 10}, fetchIDs(filtered))
	require.Equal(t, 1, det.calls, "HasUncommittedID should be called exactly once for the large gap")
}

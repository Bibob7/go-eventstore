package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gofrs/uuid/v5"
	"github.com/stretchr/testify/require"

	"github.com/Bibob7/go-eventstore"
	"github.com/Bibob7/go-eventstore/filter"
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

func TestEventStore_FetchBatchOfEventsSince(t *testing.T) {
	tests := []struct {
		name        string
		committed   []int64 // IDs inserted as committed events
		uncommitted []int64 // IDs inserted inside an open (never-committed) transaction
		since       int64
		limit       int
		wantIDs     []int64
	}{
		{
			name:      "no gaps returns all events",
			committed: []int64{1, 2, 3},
			since:     -1,
			limit:     10,
			wantIDs:   []int64{1, 2, 3},
		},
		{
			name:        "gap with one uncommitted row stops before gap",
			committed:   []int64{1, 2, 5},
			uncommitted: []int64{3},
			since:       0,
			limit:       10,
			wantIDs:     []int64{1, 2},
		},
		{
			name:        "gap with multiple uncommitted rows stops before gap",
			committed:   []int64{1, 2, 12},
			uncommitted: []int64{3, 4, 5, 6, 7, 10},
			since:       0,
			limit:       10,
			wantIDs:     []int64{1, 2},
		},
		{
			name:      "gap without uncommitted rows continues past gap",
			committed: []int64{1, 2, 5000},
			since:     0,
			limit:     10,
			wantIDs:   []int64{1, 2, 5000},
		},
		{
			name:      "huge gap without uncommitted rows continues past gap",
			committed: []int64{1, 2, 5},
			since:     0,
			limit:     10,
			wantIDs:   []int64{1, 2, 5},
		},
		{
			name:      "since skips already-seen events and continues past committed gap",
			committed: []int64{3, 4, 6},
			since:     2,
			limit:     10,
			wantIDs:   []int64{3, 4, 6},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			defer func() { _ = db.Close() }()
			ensureOutboxTable(t, db)

			for _, id := range tc.committed {
				insertEvent(t, db, id, "test")
			}

			if len(tc.uncommitted) > 0 {
				tx, err := db.Begin()
				require.NoError(t, err)
				defer func() { _ = tx.Rollback() }()
				for _, id := range tc.uncommitted {
					insertEvent(t, tx, id, "test")
				}
			}

			store := NewEventStore(db, outboxTable)
			events, err := store.FetchBatchOfEventsSince(context.Background(), tc.since, tc.limit)
			require.NoError(t, err)
			require.Equal(t, tc.wantIDs, fetchIDs(events))
		})
	}
}

// TestEventStore_GapDetection_RepeatableAttemptAfterCommit verifies that once
// uncommitted rows are committed, a subsequent fetch returns all events including
// those that were previously hidden behind the gap.
func TestEventStore_GapDetection_RepeatableAttemptAfterCommit(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()
	ensureOutboxTable(t, db)

	insertEvent(t, db, 1, "test")
	insertEvent(t, db, 2, "test")
	insertEvent(t, db, 12, "test")

	tx, err := db.Begin()
	require.NoError(t, err)
	for _, id := range []int64{3, 4, 5, 6, 7, 10} {
		insertEvent(t, tx, id, "test")
	}

	store := NewEventStore(db, outboxTable)
	ctx := context.Background()

	firstEvents, err := store.FetchBatchOfEventsSince(ctx, 0, 10)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, fetchIDs(firstEvents), "before commit: stops before gap")

	require.NoError(t, tx.Commit())

	secondEvents, err := store.FetchBatchOfEventsSince(ctx, 0, 10)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 10, 12}, fetchIDs(secondEvents), "after commit: all events visible")
}

// testEvent is a minimal DomainEvent implementation for round-trip tests.
type testEvent struct {
	EventID     uuid.UUID `json:"event_id"`
	AggregateId uuid.UUID `json:"aggregate_id"`
	Payload     string    `json:"payload"`
	OccurredOn  time.Time `json:"occurred_on"`
}

func (e *testEvent) ID() uuid.UUID          { return e.EventID }
func (e *testEvent) AggregateID() uuid.UUID { return e.AggregateId }
func (e *testEvent) EventType() string      { return "test.event" }
func (e *testEvent) OccurredAt() time.Time  { return e.OccurredOn }

// TestEventStore_AppendFetchRoundTrip verifies that Append followed by
// FetchBatchOfEventsSince preserves all fields, including the aggregate ID
// (which historically was scanned as a string and silently turned into
// uuid.Nil) and OccurredAt (which must be timezone-stable).
func TestEventStore_AppendFetchRoundTrip(t *testing.T) {
	berlin, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)

	tests := []struct {
		name     string
		occurred time.Time
	}{
		{name: "UTC timestamp", occurred: time.Date(2026, 4, 5, 12, 34, 56, 0, time.UTC)},
		{name: "local timezone timestamp", occurred: time.Date(2026, 4, 5, 14, 0, 0, 0, berlin)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			defer func() { _ = db.Close() }()
			ensureOutboxTable(t, db)

			eventID, err := uuid.NewV4()
			require.NoError(t, err)
			aggregateID, err := uuid.NewV4()
			require.NoError(t, err)

			evt := &testEvent{
				EventID:     eventID,
				AggregateId: aggregateID,
				Payload:     "hello",
				OccurredOn:  tc.occurred,
			}

			store := NewEventStore(db, outboxTable)
			ctx := context.Background()

			require.NoError(t, store.Append(ctx, evt))

			events, err := store.FetchBatchOfEventsSince(ctx, -1, 10)
			require.NoError(t, err)
			require.Len(t, events, 1)

			got := events[0]
			require.Equal(t, eventID, got.ID, "event ID must round-trip")
			require.Equal(t, aggregateID, got.EntityID, "aggregate ID must round-trip (not uuid.Nil)")
			require.NotEqual(t, uuid.Nil, got.EntityID, "aggregate ID must not be zero UUID")
			require.Equal(t, "test.event", got.EventType)
			require.True(t, got.OccurredAt.Equal(tc.occurred),
				"occurred_at must be equal regardless of timezone: got %s, want %s",
				got.OccurredAt, tc.occurred)
			require.Equal(t, time.UTC, got.OccurredAt.Location(),
				"occurred_at must be returned in UTC")
		})
	}
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

// TestEventStore_HasUncommittedCalledOnceForLargeGap ensures that HasUncommittedID
// is called exactly once per gap range, not once per missing ID.
func TestEventStore_HasUncommittedCalledOnceForLargeGap(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()
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

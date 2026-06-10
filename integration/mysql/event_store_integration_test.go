package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gofrs/uuid/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Bibob7/go-eventstore"
)

const eventStoreTable = "event_store"

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

// ensureEventStoreTable makes sure the event store table exists with the expected schema (compatible with our EventStore)
func ensureEventStoreTable(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create table if not exists. Schema mirrored from sql/mysql/schema.sql
	stmt := `CREATE TABLE IF NOT EXISTS event_store (
        id INT NOT NULL AUTO_INCREMENT,
        event_id BINARY(16) NOT NULL,
        stream_id BINARY(16) NOT NULL,
        stream_version INT NOT NULL DEFAULT 0,
        event_type VARCHAR(255) NOT NULL,
        payload JSON NOT NULL,
        occurred_at DATETIME NOT NULL,
        metadata JSON NULL,
        PRIMARY KEY (id),
        KEY stream_id_idx (stream_id),
        KEY event_type_idx (event_type),
        KEY event_id_idx (event_id),
        KEY stream_version_idx (stream_id, stream_version)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`

	_, err := db.ExecContext(ctx, stmt)
	require.NoError(t, err)

	// Clean slate for each test run
	_, err = db.ExecContext(ctx, "TRUNCATE TABLE "+eventStoreTable)
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
	streamID, _ := uuid.NewV4()
	payload := "{}"
	occurredAt := time.Now().Format(time.DateTime)

	// Explicitly set id to craft gaps
	stmt := fmt.Sprintf("INSERT INTO %s (id, event_id, stream_id, event_type, payload, occurred_at) VALUES (?, ?, ?, ?, ?, ?)", eventStoreTable)
	_, err := exec.ExecContext(context.Background(), stmt, id, mustBinary(evtID), mustBinary(streamID), eventType, payload, occurredAt)
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
			ensureEventStoreTable(t, db)

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

			store := NewEventStore(db, eventStoreTable)
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
	ensureEventStoreTable(t, db)

	insertEvent(t, db, 1, "test")
	insertEvent(t, db, 2, "test")
	insertEvent(t, db, 12, "test")

	tx, err := db.Begin()
	require.NoError(t, err)
	for _, id := range []int64{3, 4, 5, 6, 7, 10} {
		insertEvent(t, tx, id, "test")
	}

	store := NewEventStore(db, eventStoreTable)
	ctx := context.Background()

	firstEvents, err := store.FetchBatchOfEventsSince(ctx, 0, 10)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, fetchIDs(firstEvents), "before commit: stops before gap")

	require.NoError(t, tx.Commit())

	secondEvents, err := store.FetchBatchOfEventsSince(ctx, 0, 10)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 10, 12}, fetchIDs(secondEvents), "after commit: all events visible")
}

// TestEventStore_GapDetection_WithConcurrentTransactions verifies that a later
// committed event with a higher auto-increment ID remains hidden while an
// earlier transaction still holds the lower ID uncommitted.
func TestEventStore_GapDetection_WithConcurrentTransactions(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()
	ensureEventStoreTable(t, db)

	store := NewEventStore(db, eventStoreTable)
	ctx := context.Background()

	tx1, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx1.Rollback() }()

	firstEvent := &testEvent{
		EventID:    mustUUID(t),
		StreamId:   mustUUID(t),
		Payload:    "first",
		OccurredOn: time.Now(),
	}
	require.NoError(t, store.Append(WithTx(ctx, tx1), firstEvent))

	tx2, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx2.Rollback() }()

	secondEvent := &testEvent{
		EventID:    mustUUID(t),
		StreamId:   mustUUID(t),
		Payload:    "second",
		OccurredOn: time.Now(),
	}
	require.NoError(t, store.Append(WithTx(ctx, tx2), secondEvent))
	require.NoError(t, tx2.Commit())

	blockedEvents, err := store.FetchBatchOfEventsSince(ctx, 0, 10)
	require.NoError(t, err)
	require.Empty(t, blockedEvents, "the committed higher increment ID must stay hidden behind the uncommitted gap")

	require.NoError(t, tx1.Commit())

	eventsAfterCommit, err := store.FetchBatchOfEventsSince(ctx, 0, 10)
	require.NoError(t, err)
	require.Len(t, eventsAfterCommit, 2)
	require.Equal(t, []int64{1, 2}, fetchIDs(eventsAfterCommit))
	require.Equal(t, firstEvent.ID(), eventsAfterCommit[0].ID)
	require.Equal(t, secondEvent.ID(), eventsAfterCommit[1].ID)
}

// testEvent is a minimal DomainEvent implementation for round-trip tests.
//
// The metadata field is named `Meta` to avoid a clash with the `Metadata()`
// method: Go disallows a field and a method with the same name on the same
// type. The method returns the field as eventstore.Metadata, which is what
// the DomainEvent interface expects.
type testEvent struct {
	EventID    uuid.UUID           `json:"event_id"`
	StreamId   uuid.UUID           `json:"stream_id"`
	Payload    string              `json:"payload"`
	OccurredOn time.Time           `json:"occurred_on"`
	Meta       eventstore.Metadata `json:"metadata,omitempty"`
}

func (e *testEvent) ID() uuid.UUID                 { return e.EventID }
func (e *testEvent) StreamID() uuid.UUID           { return e.StreamId }
func (e *testEvent) EventType() string             { return "test.event" }
func (e *testEvent) OccurredAt() time.Time         { return e.OccurredOn }
func (e *testEvent) Metadata() eventstore.Metadata { return e.Meta }

func mustUUID(t *testing.T) uuid.UUID {
	t.Helper()
	id, err := uuid.NewV4()
	require.NoError(t, err)
	return id
}

// TestEventStore_AppendFetchRoundTrip verifies that Append followed by
// FetchBatchOfEventsSince preserves all fields, including the stream ID
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
			ensureEventStoreTable(t, db)

			eventID, err := uuid.NewV4()
			require.NoError(t, err)
			streamID, err := uuid.NewV4()
			require.NoError(t, err)

			evt := &testEvent{
				EventID:    eventID,
				StreamId:   streamID,
				Payload:    "hello",
				OccurredOn: tc.occurred,
			}

			store := NewEventStore(db, eventStoreTable)
			ctx := context.Background()

			require.NoError(t, store.Append(ctx, evt))

			events, err := store.FetchBatchOfEventsSince(ctx, -1, 10)
			require.NoError(t, err)
			require.Len(t, events, 1)

			got := events[0]
			require.Equal(t, eventID, got.ID, "event ID must round-trip")
			require.Equal(t, streamID, got.StreamID, "stream ID must round-trip (not uuid.Nil)")
			require.NotEqual(t, uuid.Nil, got.StreamID, "stream ID must not be zero UUID")
			require.Equal(t, "test.event", got.EventType)
			require.True(t, got.OccurredAt.Equal(tc.occurred),
				"occurred_at must be equal regardless of timezone: got %s, want %s",
				got.OccurredAt, tc.occurred)
			require.Equal(t, time.UTC, got.OccurredAt.Location(),
				"occurred_at must be returned in UTC")
		})
	}
}

// TestEventStore_MetadataRoundTrip verifies that Metadata is persisted on
// Append and recovered on FetchBatchOfEventsSince. Three cases matter:
//
//  1. nil metadata is stored as SQL NULL and read back as nil (not an empty
//     map) — keeps the wire format cheap when no metadata is attached.
//  2. an empty Metadata is treated equivalently to nil.
//  3. a populated Metadata round-trips byte-for-byte as a JSON object, so
//     consumers can rely on the conventional keys (correlation_id,
//     causation_id, trace_id) and on custom keys alike.
func TestEventStore_MetadataRoundTrip(t *testing.T) {
	tests := []struct {
		name        string
		metadata    eventstore.Metadata
		wantNil     bool
		wantEntries map[string]string
	}{
		{
			name:     "nil metadata round-trips as nil",
			metadata: nil,
			wantNil:  true,
		},
		{
			name:     "empty metadata round-trips as nil (no JSON overhead)",
			metadata: eventstore.Metadata{},
			wantNil:  true,
		},
		{
			name: "populated metadata round-trips as JSON object",
			metadata: eventstore.Metadata{
				eventstore.MetadataKeyCorrelationID: "corr-123",
				eventstore.MetadataKeyCausationID:   "evt-prev-456",
				eventstore.MetadataKeyTraceID:       "trace-789",
			},
			wantNil: false,
			wantEntries: map[string]string{
				eventstore.MetadataKeyCorrelationID: "corr-123",
				eventstore.MetadataKeyCausationID:   "evt-prev-456",
				eventstore.MetadataKeyTraceID:       "trace-789",
			},
		},
		{
			name: "custom (non-reserved) keys round-trip too",
			metadata: eventstore.Metadata{
				"tenant_id": "acme",
				"actor_id":  "user-42",
			},
			wantNil: false,
			wantEntries: map[string]string{
				"tenant_id": "acme",
				"actor_id":  "user-42",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			defer func() { _ = db.Close() }()
			ensureEventStoreTable(t, db)

			eventID := mustUUID(t)
			streamID := mustUUID(t)

			evt := &testEvent{
				EventID:    eventID,
				StreamId:   streamID,
				Payload:    "hello",
				OccurredOn: time.Now().UTC(),
				Meta:       tc.metadata,
			}

			store := NewEventStore(db, eventStoreTable)
			ctx := context.Background()
			require.NoError(t, store.Append(ctx, evt))

			events, err := store.FetchBatchOfEventsSince(ctx, -1, 10)
			require.NoError(t, err)
			require.Len(t, events, 1)

			got := events[0].Metadata
			if tc.wantNil {
				require.Nil(t, got,
					"expected nil metadata, got %#v (nil and empty map must be equivalent)", got)
				return
			}
			require.NotNil(t, got, "expected non-nil metadata")
			require.Equal(t, tc.wantEntries, map[string]string(got))
		})
	}
}

// TestEventStore_MetadataPersistedAsNullColumn verifies directly against the
// column that nil/empty metadata is stored as SQL NULL, not as an empty JSON
// object. This protects the perf claim in the Append implementation.
func TestEventStore_MetadataPersistedAsNullColumn(t *testing.T) {
	tests := []struct {
		name     string
		metadata eventstore.Metadata
	}{
		{name: "nil metadata", metadata: nil},
		{name: "empty metadata", metadata: eventstore.Metadata{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			defer func() { _ = db.Close() }()
			ensureEventStoreTable(t, db)

			evt := &testEvent{
				EventID:    mustUUID(t),
				StreamId:   mustUUID(t),
				Payload:    "hello",
				OccurredOn: time.Now().UTC(),
				Meta:       tc.metadata,
			}

			store := NewEventStore(db, eventStoreTable)
			require.NoError(t, store.Append(context.Background(), evt))

			var raw sql.NullString
			row := db.QueryRow("SELECT metadata FROM " + eventStoreTable + " ORDER BY id DESC LIMIT 1")
			require.NoError(t, row.Scan(&raw))
			require.False(t, raw.Valid, "metadata column must be SQL NULL for nil/empty input")
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
	ensureEventStoreTable(t, db)

	insertEvent(t, db, 1, "test")
	insertEvent(t, db, 2, "test")
	insertEvent(t, db, 10, "test")

	store := NewEventStore(db, eventStoreTable)
	det := &countingGapDetector{store: store}

	input := []eventstore.StoredEvent{
		{IncrementID: 1},
		{IncrementID: 2},
		{IncrementID: 10},
	}

	ctx := context.Background()
	filtered, err := eventstore.NewUntilGapEventFilter(0, det).Execute(ctx, input)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 10}, fetchIDs(filtered))
	require.Equal(t, 1, det.calls, "HasUncommittedID should be called exactly once for the large gap")
}

// TestEventStore_CleanUpToIncluding verifies that CleanUpToIncluding removes
// every event with IncrementID less than or equal to the given threshold,
// including the threshold itself.
func TestEventStore_CleanUpToIncluding(t *testing.T) {
	tests := []struct {
		name      string
		seed      []int64
		threshold int64
		wantIDs   []int64
	}{
		{
			name:      "removes everything up to and including threshold",
			seed:      []int64{1, 2, 3, 4, 5},
			threshold: 3,
			wantIDs:   []int64{4, 5},
		},
		{
			name:      "threshold equal to highest id removes everything",
			seed:      []int64{1, 2, 3},
			threshold: 3,
			wantIDs:   nil,
		},
		{
			name:      "threshold equal to lowest id removes it",
			seed:      []int64{1, 2, 3},
			threshold: 1,
			wantIDs:   []int64{2, 3},
		},
		{
			name:      "threshold below lowest id removes nothing",
			seed:      []int64{1, 2, 3},
			threshold: 0,
			wantIDs:   []int64{1, 2, 3},
		},
		{
			name:      "threshold above highest id removes everything",
			seed:      []int64{1, 2, 3},
			threshold: 100,
			wantIDs:   nil,
		},
		{
			name:      "no rows means no-op",
			seed:      nil,
			threshold: 5,
			wantIDs:   nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			defer func() { _ = db.Close() }()
			ensureEventStoreTable(t, db)

			for _, id := range tc.seed {
				insertEvent(t, db, id, "test")
			}

			store := NewEventStore(db, eventStoreTable)
			require.NoError(t, store.CleanUpToIncluding(context.Background(), tc.threshold))

			remaining, err := store.FetchBatchOfEventsSince(context.Background(), -1, 100)
			require.NoError(t, err)
			require.Equal(t, tc.wantIDs, fetchIDs(remaining))
		})
	}
}

// newTestEvent returns a testEvent pinned to the given stream. The caller
// supplies the payload; the helper assembles the rest so each test row
// stays focused on what differs.
func newTestEvent(t *testing.T, streamID uuid.UUID, payload string) *testEvent {
	t.Helper()
	return &testEvent{
		EventID:    mustUUID(t),
		StreamId:   streamID,
		Payload:    payload,
		OccurredOn: time.Now().UTC(),
	}
}

// TestEventStore_AppendWithExpectedVersion_AcceptsContiguousSequence is
// the happy-path integration test: appending three events to a fresh
// stream with expectedVersion = -1 (then 0, 1) persists all of them with
// consecutive stream versions 0, 1, 2.
func TestEventStore_AppendWithExpectedVersion_AcceptsContiguousSequence(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()
	ensureEventStoreTable(t, db)

	streamID := mustUUID(t)
	store := NewEventStore(db, eventStoreTable)
	ctx := context.Background()

	// expectedVersion = -1 means "stream must be empty". After this, the
	// stream's head is at version 0.
	require.NoError(t, store.AppendWithExpectedVersion(ctx, streamID, -1,
		newTestEvent(t, streamID, "first"),
		newTestEvent(t, streamID, "second"),
		newTestEvent(t, streamID, "third"),
	))

	rows, err := store.ReadStream(ctx, streamID, 0, 10)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	require.Equal(t, []int{0, 1, 2}, []int{
		rows[0].StreamVersion, rows[1].StreamVersion, rows[2].StreamVersion,
	})
}

// TestEventStore_AppendWithExpectedVersion_RejectsConflicts is the
// table-driven conflict suite. Each row seeds a stream, then attempts a
// second AppendWithExpectedVersion with a wrong expectedVersion and
// expects an *eventstore.StreamVersionConflictError whose diagnostics
// (StreamID, Expected, Got) match the row.
func TestEventStore_AppendWithExpectedVersion_RejectsConflicts(t *testing.T) {
	tests := []struct {
		name         string
		seedCount    int // how many events to seed on the stream (versions 0..seedCount-1)
		expectedAt   int // expectedVersion passed to the second call
		wantExpected int // expected current head reported in the conflict
		wantGot      int // actual current head reported in the conflict
	}{
		{
			name:         "expectedVersion 0 against a stream with two events (head = 1)",
			seedCount:    2,
			expectedAt:   0,
			wantExpected: 0,
			wantGot:      1,
		},
		{
			name:         "expectedVersion 5 against a stream with two events (head = 1)",
			seedCount:    2,
			expectedAt:   5,
			wantExpected: 5,
			wantGot:      1,
		},
		{
			name:         "expectedVersion -1 (empty) against a non-empty stream",
			seedCount:    1,
			expectedAt:   -1,
			wantExpected: -1,
			wantGot:      0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			defer func() { _ = db.Close() }()
			ensureEventStoreTable(t, db)

			streamID := mustUUID(t)
			store := NewEventStore(db, eventStoreTable)
			ctx := context.Background()

			if tc.seedCount > 0 {
				seed := make([]eventstore.DomainEvent, tc.seedCount)
				for i := 0; i < tc.seedCount; i++ {
					seed[i] = newTestEvent(t, streamID, "seed")
				}
				require.NoError(t, store.AppendWithExpectedVersion(ctx, streamID, -1, seed...))
			}

			evt := newTestEvent(t, streamID, "offender")
			err := store.AppendWithExpectedVersion(ctx, streamID, tc.expectedAt, evt)
			require.Error(t, err, "expected a conflict error")

			var conflict *eventstore.StreamVersionConflictError
			require.ErrorAs(t, err, &conflict,
				"AppendWithExpectedVersion must return a *StreamVersionConflictError, got %T: %v", err, err)
			assert.Equal(t, streamID, conflict.StreamID)
			assert.Equal(t, tc.wantExpected, conflict.Expected)
			assert.Equal(t, tc.wantGot, conflict.Got)
			assert.True(t, errors.Is(err, eventstore.ErrStreamVersionConflict))
		})
	}
}

// TestEventStore_AppendWithExpectedVersion_RejectsForeignStreamEvent
// verifies that an event whose StreamID() does not match the streamID
// parameter is reported as a *StreamVersionConflictError, not silently
// inserted. This is the per-batch single-stream invariant: a StreamStore
// is the gatekeeper for one stream, and a mixed batch is a bug.
func TestEventStore_AppendWithExpectedVersion_RejectsForeignStreamEvent(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()
	ensureEventStoreTable(t, db)

	streamA := mustUUID(t)
	streamB := mustUUID(t)
	store := NewEventStore(db, eventStoreTable)
	ctx := context.Background()

	evtA := newTestEvent(t, streamA, "for-a")
	evtB := newTestEvent(t, streamB, "actually-for-b")
	err := store.AppendWithExpectedVersion(ctx, streamA, -1, evtA, evtB)
	require.Error(t, err)
	var conflict *eventstore.StreamVersionConflictError
	require.ErrorAs(t, err, &conflict)
	assert.Equal(t, streamA, conflict.StreamID)
	assert.Equal(t, evtB.ID(), conflict.EventID)
	// Got is set to -1 to signal "wrong stream for this batch".
	assert.Equal(t, -1, conflict.Got)
}

// TestEventStore_AppendWithExpectedVersion_StreamsAreIndependent
// verifies that events on stream A do not affect the version counter of
// stream B — i.e. a successful append to A followed by a successful append
// to B works in either order, and a conflict on A does not roll back the
// state of B. Because each AppendWithExpectedVersion owns its own
// transaction, "rollback" is per-call, not cross-stream.
func TestEventStore_AppendWithExpectedVersion_StreamsAreIndependent(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()
	ensureEventStoreTable(t, db)

	streamA := mustUUID(t)
	streamB := mustUUID(t)
	store := NewEventStore(db, eventStoreTable)
	ctx := context.Background()

	// streamA gets versions 0, 1.
	require.NoError(t, store.AppendWithExpectedVersion(ctx, streamA, -1,
		newTestEvent(t, streamA, "a0"),
		newTestEvent(t, streamA, "a1"),
	))
	// streamB starts empty and gets version 0.
	require.NoError(t, store.AppendWithExpectedVersion(ctx, streamB, -1,
		newTestEvent(t, streamB, "b0"),
	))

	aEvents, err := store.ReadStream(ctx, streamA, 0, 10)
	require.NoError(t, err)
	require.Len(t, aEvents, 2)
	bEvents, err := store.ReadStream(ctx, streamB, 0, 10)
	require.NoError(t, err)
	require.Len(t, bEvents, 1)

	// A stale-version attempt on streamA must fail and report A's head.
	err = store.AppendWithExpectedVersion(ctx, streamA, 0, // stale: head is 1
		newTestEvent(t, streamA, "a-stale"))
	require.Error(t, err)
	var conflict *eventstore.StreamVersionConflictError
	require.ErrorAs(t, err, &conflict)
	assert.Equal(t, streamA, conflict.StreamID)
	assert.Equal(t, 0, conflict.Expected)
	assert.Equal(t, 1, conflict.Got)

	// streamB is unaffected.
	require.NoError(t, store.AppendWithExpectedVersion(ctx, streamB, 0,
		newTestEvent(t, streamB, "b1")))
	bEvents, err = store.ReadStream(ctx, streamB, 0, 10)
	require.NoError(t, err)
	require.Len(t, bEvents, 2)
	assert.Equal(t, 0, bEvents[0].StreamVersion)
	assert.Equal(t, 1, bEvents[1].StreamVersion)
}

// TestEventStore_ReadStream_FromVersionInclusive verifies the inklusiv
// semantics: ReadStream(streamID, fromVersion, limit) includes the event
// at fromVersion itself.
func TestEventStore_ReadStream_FromVersionInclusive(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()
	ensureEventStoreTable(t, db)

	streamID := mustUUID(t)
	store := NewEventStore(db, eventStoreTable)
	ctx := context.Background()

	require.NoError(t, store.AppendWithExpectedVersion(ctx, streamID, -1,
		newTestEvent(t, streamID, "v0"),
		newTestEvent(t, streamID, "v1"),
		newTestEvent(t, streamID, "v2"),
		newTestEvent(t, streamID, "v3"),
	))

	tests := []struct {
		name        string
		fromVersion int
		limit       int
		wantCount   int
		wantFirst   int
		wantLast    int
	}{
		{name: "from 0 returns the entire stream", fromVersion: 0, limit: 10, wantCount: 4, wantFirst: 0, wantLast: 3},
		{name: "from 2 returns the tail", fromVersion: 2, limit: 10, wantCount: 2, wantFirst: 2, wantLast: 3},
		{name: "from 1 with limit 1 returns only v1", fromVersion: 1, limit: 1, wantCount: 1, wantFirst: 1, wantLast: 1},
		{name: "from past the end returns empty", fromVersion: 99, limit: 10, wantCount: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := store.ReadStream(ctx, streamID, tc.fromVersion, tc.limit)
			require.NoError(t, err)
			require.Len(t, got, tc.wantCount)
			if tc.wantCount > 0 {
				assert.Equal(t, tc.wantFirst, got[0].StreamVersion,
					"first event version must equal fromVersion (inklusiv)")
				assert.Equal(t, tc.wantLast, got[len(got)-1].StreamVersion)
			}
		})
	}
}

// TestEventStore_ReadStream_OtherStreamUnaffected verifies that ReadStream
// never returns events of other streams, even when a higher stream_version
// exists in the table.
func TestEventStore_ReadStream_OtherStreamUnaffected(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()
	ensureEventStoreTable(t, db)

	store := NewEventStore(db, eventStoreTable)
	ctx := context.Background()

	streamA := mustUUID(t)
	streamB := mustUUID(t)
	require.NoError(t, store.AppendWithExpectedVersion(ctx, streamA, -1,
		newTestEvent(t, streamA, "a0"),
		newTestEvent(t, streamA, "a1"),
	))
	require.NoError(t, store.AppendWithExpectedVersion(ctx, streamB, -1,
		newTestEvent(t, streamB, "b0"),
	))

	got, err := store.ReadStream(ctx, streamA, 0, 100)
	require.NoError(t, err)
	require.Len(t, got, 2)
	for _, e := range got {
		assert.Equal(t, streamA, e.StreamID)
	}
}

// TestEventStore_Append_PlainAppendOnly verifies that Store.Append (the
// plain append-only-log path, no per-stream ordering enforced) just
// inserts events regardless of their StreamID. It is the right path for
// outbox / projection workloads that don't model aggregates.
func TestEventStore_Append_PlainAppendOnly(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()
	ensureEventStoreTable(t, db)

	store := NewEventStore(db, eventStoreTable)
	ctx := context.Background()

	streamA := mustUUID(t)
	streamB := mustUUID(t)
	// Mixed-stream batch: Store.Append has no opinion on StreamID.
	require.NoError(t, store.Append(ctx,
		newTestEvent(t, streamA, "a0"),
		newTestEvent(t, streamB, "b0"),
		newTestEvent(t, streamA, "a1"),
	))

	rows, err := store.ReadStream(ctx, streamA, 0, 100)
	require.NoError(t, err)
	assert.Len(t, rows, 2)
}

// TestEventStore_LatestStreamVersion verifies that the cheap pre-check
// returns the highest persisted stream_version for a stream, or -1 for an
// empty one. The contract is:
//   - unknown streamID  → -1, nil (caller treats it as a fresh stream)
//   - single event      →  0
//   - multiple events   →  max(stream_version)
//   - other streams' events are ignored even when their versions are higher
func TestEventStore_LatestStreamVersion(t *testing.T) {
	tests := []struct {
		name   string
		seeded map[string]int // streamID-as-hex → how many events to seed (versions 0..N-1)
		query  string         // streamID-as-hex to query; "" means "fresh stream"
		want   int
	}{
		{
			name:   "unknown stream returns -1 sentinel",
			seeded: nil,
			query:  "",
			want:   -1,
		},
		{
			name:   "single event returns 0",
			seeded: map[string]int{"only": 1},
			query:  "only",
			want:   0,
		},
		{
			name:   "multiple events return max, not last-appended",
			seeded: map[string]int{"s": 5},
			query:  "s",
			want:   4,
		},
		{
			name: "other streams with higher versions do not affect this stream",
			seeded: map[string]int{
				"a": 2,
				"b": 6,
			},
			query: "a",
			want:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			defer func() { _ = db.Close() }()
			ensureEventStoreTable(t, db)

			store := NewEventStore(db, eventStoreTable)
			ctx := context.Background()

			// Resolve hex stream IDs from the table to real UUIDs.
			ids := make(map[string]uuid.UUID, len(tc.seeded))
			for hex, n := range tc.seeded {
				id := mustUUID(t)
				ids[hex] = id
				batch := make([]eventstore.DomainEvent, n)
				for i := 0; i < n; i++ {
					batch[i] = newTestEvent(t, id, "seed")
				}
				require.NoError(t, store.AppendWithExpectedVersion(ctx, id, -1, batch...))
			}

			var queryID uuid.UUID
			if tc.query == "" {
				queryID = mustUUID(t)
			} else {
				queryID = ids[tc.query]
			}

			got, err := store.LatestStreamVersion(ctx, queryID)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got,
				"LatestStreamVersion must return max(stream_version), or -1 for empty streams")
		})
	}
}

// TestEventStore_LatestStreamVersion_WorksOnStoreAppend verifies that
// LatestStreamVersion is a read-side query that does not depend on which
// write path produced the events — events inserted via the plain
// Store.Append path are just as visible as events written via
// AppendWithExpectedVersion. This locks down the separation between the
// write paths and the read-side stream view.
func TestEventStore_LatestStreamVersion_WorksOnStoreAppend(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()
	ensureEventStoreTable(t, db)

	store := NewEventStore(db, eventStoreTable)
	ctx := context.Background()

	streamID := mustUUID(t)
	require.NoError(t, store.Append(ctx,
		newTestEvent(t, streamID, "a"),
		newTestEvent(t, streamID, "b"),
		newTestEvent(t, streamID, "c"),
	))

	got, err := store.LatestStreamVersion(ctx, streamID)
	require.NoError(t, err)
	// Three rows, but Store.Append writes them with stream_version = 0
	// (the append-only path does not enforce per-stream ordering). The
	// MAX is therefore 0, not 2. This is the documented difference
	// between the two write paths: LatestStreamVersion reports what the
	// store actually persisted, not what the caller "intended".
	assert.Equal(t, 0, got)
}

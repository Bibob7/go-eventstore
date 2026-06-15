package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/gofrs/uuid/v5"

	"github.com/Bibob7/go-eventstore"
)

// mysqlErrDuplicateKey is MySQL error 1062 (ER_DUP_ENTRY), raised when an
// insert violates a UNIQUE index — here: (stream_id, stream_version).
const mysqlErrDuplicateKey = 1062

// occurredAtFormat is the DATETIME(6) wire format: always write six
// fractional digits so sub-second precision survives the round-trip.
const occurredAtFormat = "2006-01-02 15:04:05.000000"

type EventStore struct {
	db        *sql.DB
	tableName string
}

// NewEventStore constructs an EventStore bound to the given database and table.
// It panics if tableName is not a valid SQL identifier. The returned value
// satisfies eventstore.Store, StreamStore, StreamReader, and
// StreamVersionReader; depend on whichever fits your use case.
func NewEventStore(db *sql.DB, tableName string) *EventStore {
	mustValidateIdentifier("tableName", tableName)
	return &EventStore{
		db:        db,
		tableName: tableName,
	}
}

// Append persists the given events as a plain append-only log, joining the
// caller's transaction from ctx if present. For per-stream optimistic
// concurrency use AppendWithExpectedVersion instead.
func (s *EventStore) Append(ctx context.Context, domainEvents ...eventstore.DomainEvent) error {
	if len(domainEvents) == 0 {
		return nil
	}
	run := func(tx *sql.Tx) error {
		return s.insertEvents(ctx, tx, domainEvents, nil)
	}
	if tx, exists := GetTx(ctx); exists {
		slog.Debug("Appending domainEvents to DomainEvent eventStore in transaction")
		return run(tx)
	}
	slog.Debug("Appending domainEvents to DomainEvent eventStore without transaction")
	return WithTransaction(ctx, s.db, run, nil)
}

// AppendWithExpectedVersion atomically verifies that the stream's head equals
// expectedVersion and appends the events at consecutive versions from
// expectedVersion + 1. It satisfies eventstore.StreamStore.
//
// expectedVersion of -1 requires an empty stream (the create path); N >= 0
// requires the head to be exactly N. A malformed call (wrong streamID or
// expectedVersion < -1) returns an error wrapping
// eventstore.ErrInvalidStreamAppend.
//
// The pre-check runs in the same transaction as the insert (caller-supplied via
// WithTx/GetTx, or store-owned). Because it is a non-locking read, a concurrent
// writer may also pass it; the UNIQUE index on (stream_id, stream_version) then
// rejects the loser. Either way a conflict is reported as a
// *StreamVersionConflictError and the transaction is rolled back.
func (s *EventStore) AppendWithExpectedVersion(
	ctx context.Context,
	streamID uuid.UUID,
	expectedVersion int,
	domainEvents ...eventstore.DomainEvent,
) error {
	if expectedVersion < -1 {
		return fmt.Errorf("%w: expectedVersion %d is below the -1 sentinel (stream %s)",
			eventstore.ErrInvalidStreamAppend, expectedVersion, streamID)
	}
	if len(domainEvents) == 0 {
		return nil
	}
	// All events must belong to streamID (StreamStore contract); a mismatch is
	// a programming error, not a concurrency conflict.
	for _, ev := range domainEvents {
		if ev.StreamID() != streamID {
			return fmt.Errorf("%w: event %s belongs to stream %s, not %s",
				eventstore.ErrInvalidStreamAppend, ev.ID(), ev.StreamID(), streamID)
		}
	}

	run := func(tx *sql.Tx) error {
		lastVersion, err := s.currentStreamVersion(ctx, tx, streamID)
		if err != nil {
			return err
		}
		if lastVersion != expectedVersion {
			return &eventstore.StreamVersionConflictError{
				StreamID: streamID,
				EventID:  domainEvents[0].ID(),
				Expected: expectedVersion,
				Got:      lastVersion,
			}
		}
		// Per-event version assignments: expectedVersion+1, +2, …
		versions := make([]int, len(domainEvents))
		for i := range domainEvents {
			versions[i] = expectedVersion + 1 + i
		}
		return s.insertEvents(ctx, tx, domainEvents, versions)
	}

	var err error
	if tx, exists := GetTx(ctx); exists {
		err = run(tx)
	} else {
		err = WithTransaction(ctx, s.db, run, nil)
	}
	return s.mapDuplicateVersionError(ctx, err, streamID, expectedVersion, domainEvents[0].ID())
}

// mapDuplicateVersionError translates a duplicate-key violation on the
// (stream_id, stream_version) UNIQUE index into a *StreamVersionConflictError.
// The constraint, not the non-locking pre-check in AppendWithExpectedVersion,
// is what ultimately enforces the optimistic-concurrency contract when two
// writers race to insert the same version.
func (s *EventStore) mapDuplicateVersionError(ctx context.Context, err error, streamID uuid.UUID, expectedVersion int, firstEventID uuid.UUID) error {
	var mysqlErr *gomysql.MySQLError
	if !errors.As(err, &mysqlErr) || mysqlErr.Number != mysqlErrDuplicateKey {
		return err
	}
	// Best-effort read of the actual head for diagnostics, on a fresh connection
	// (the failed transaction's snapshot can't see the winner's commit). The
	// duplicate key proves the head is at least expectedVersion+1.
	got, readErr := s.LatestStreamVersion(ctx, streamID)
	if readErr != nil {
		got = expectedVersion + 1
	}
	return &eventstore.StreamVersionConflictError{
		StreamID: streamID,
		EventID:  firstEventID,
		Expected: expectedVersion,
		Got:      got,
	}
}

// currentStreamVersion returns the highest stream_version for streamID, or -1
// if the stream is empty. It runs inside the caller's transaction so the read
// and the subsequent insert are serialized against concurrent writers.
func (s *EventStore) currentStreamVersion(ctx context.Context, tx *sql.Tx, streamID uuid.UUID) (int, error) {
	binStreamID, err := streamID.MarshalBinary()
	if err != nil {
		return 0, err
	}
	// #nosec G201 -- tableName is validated in the constructor.
	query := fmt.Sprintf("SELECT MAX(stream_version) FROM %s WHERE stream_id = ?", s.tableName)
	var maxVersion sql.NullInt64
	if err := tx.QueryRowContext(ctx, query, binStreamID).Scan(&maxVersion); err != nil {
		return 0, err
	}
	if !maxVersion.Valid {
		return -1, nil
	}
	return int(maxVersion.Int64), nil
}

// insertEvents builds and executes the multi-row INSERT for the batch. When
// versions is non-nil it must match domainEvents in length and supplies each
// row's stream_version (StreamStore path); when nil, every row gets NULL (plain
// Append path), keeping unversioned rows out of the (stream_id, stream_version)
// UNIQUE index so the two write paths cannot collide.
func (s *EventStore) insertEvents(ctx context.Context, tx *sql.Tx, domainEvents []eventstore.DomainEvent, versions []int) error {
	if versions != nil && len(versions) != len(domainEvents) {
		return fmt.Errorf("insertEvents: versions length %d != events length %d", len(versions), len(domainEvents))
	}
	const argsNum = 7
	valuesStrings := make([]string, len(domainEvents))
	valuesArgs := make([]interface{}, len(domainEvents)*argsNum)

	for i, domainEvent := range domainEvents {
		eventPayloadJsonString := "[]"
		eventPayload, err := json.Marshal(domainEvent)
		if err != nil {
			return err
		}
		if eventPayload != nil {
			eventPayloadJsonString = string(eventPayload)
		}

		valuesStrings[i] = "(?, ?, ?, ?, ?, ?, ?)"
		j := i * argsNum
		binaryId, err := domainEvent.ID().MarshalBinary()
		if err != nil {
			return err
		}

		binaryStreamId, err := domainEvent.StreamID().MarshalBinary()
		if err != nil {
			return err
		}

		valuesArgs[j] = binaryId
		valuesArgs[j+1] = binaryStreamId
		if versions != nil {
			valuesArgs[j+2] = versions[i]
		} else {
			valuesArgs[j+2] = nil
		}
		valuesArgs[j+3] = domainEvent.EventType()
		valuesArgs[j+4] = eventPayloadJsonString
		// Persist in UTC so reads are timezone-stable.
		valuesArgs[j+5] = domainEvent.OccurredAt().UTC().Format(occurredAtFormat)
		// nil and empty metadata both map to NULL, avoiding a JSON round-trip.
		var md eventstore.Metadata
		if mp, ok := domainEvent.(eventstore.MetadataProvider); ok {
			md = mp.Metadata()
		}
		if len(md) > 0 {
			mdJSON, err := json.Marshal(map[string]string(md))
			if err != nil {
				return err
			}
			valuesArgs[j+6] = string(mdJSON)
		} else {
			valuesArgs[j+6] = nil
		}
	}

	// #nosec G201 -- tableName is validated in the constructor.
	sqlStmt := fmt.Sprintf(
		"INSERT INTO %s (event_id, stream_id, stream_version, event_type, payload, occurred_at, metadata) VALUES %s",
		s.tableName, strings.Join(valuesStrings, ","))
	_, err := tx.ExecContext(ctx, sqlStmt, valuesArgs...)
	return err
}

// FetchBatchOfEvents fetches up to limit events starting from the smallest
// IncrementID. No gap detection is applied: it is meant for transient relays
// that delete processed events, so ID gaps are expected.
func (s *EventStore) FetchBatchOfEvents(ctx context.Context, limit int) ([]eventstore.StoredEvent, error) {
	return s.fetchBatchOfEvents(ctx, -1, limit)
}

// FetchBatchOfEventsSince fetches up to limit events after lastIncrementID,
// applying gap detection.
func (s *EventStore) FetchBatchOfEventsSince(ctx context.Context, lastIncrementID int64, limit int) ([]eventstore.StoredEvent, error) {
	storedEvents, err := s.fetchBatchOfEvents(ctx, lastIncrementID, limit)
	if err != nil {
		return nil, err
	}
	return eventstore.NewUntilGapEventFilter(lastIncrementID, s).Execute(ctx, storedEvents)
}

func (s *EventStore) fetchBatchOfEvents(ctx context.Context, lastIncrementID int64, limit int) ([]eventstore.StoredEvent, error) {
	// #nosec G201 -- tableName is validated in the constructor.
	selectStmt := fmt.Sprintf(
		"SELECT id, event_id, stream_id, stream_version, event_type, payload, occurred_at, metadata FROM %s",
		s.tableName)
	queryArgs := []interface{}{limit}
	if lastIncrementID >= 0 {
		selectStmt += " WHERE id > ?"
		queryArgs = []interface{}{lastIncrementID, limit}
	}
	selectStmt += " ORDER BY id ASC LIMIT ?"
	slog.Debug("Fetching events from eventStore", "lastIncrementID", lastIncrementID, "limit", limit, "selectStmt", selectStmt)

	rows, err := s.db.QueryContext(ctx, selectStmt, queryArgs...)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("failed to close rows", "error", err)
		}
	}()

	return s.transformToStoredEvents(rows)
}

// LatestStreamVersion returns the highest StreamVersion for streamID, or -1 if
// the stream has no events. It satisfies eventstore.StreamVersionReader with a
// single SELECT MAX(stream_version), the cheap alternative to ReadStream when
// only the position is needed.
func (s *EventStore) LatestStreamVersion(ctx context.Context, streamID uuid.UUID) (int, error) {
	binStreamID, err := streamID.MarshalBinary()
	if err != nil {
		return 0, err
	}
	// #nosec G201 -- tableName is validated in the constructor.
	query := fmt.Sprintf("SELECT MAX(stream_version) FROM %s WHERE stream_id = ?", s.tableName)
	var maxVersion sql.NullInt64
	if err := s.db.QueryRowContext(ctx, query, binStreamID).Scan(&maxVersion); err != nil {
		return 0, err
	}
	if !maxVersion.Valid {
		// No row matched: the stream is empty. -1 is the sentinel so callers
		// compute the next version as last + 1.
		return -1, nil
	}
	return int(maxVersion.Int64), nil
}

// ReadStream returns up to limit events for the stream, ordered by
// StreamVersion ascending from fromVersion inclusive. It satisfies
// eventstore.StreamReader. Unversioned events (plain Append path) are never
// returned. Because the result is capped at limit, use
// eventstore.ReadStreamAll to read a long stream in full.
func (s *EventStore) ReadStream(ctx context.Context, streamID uuid.UUID, fromVersion, limit int) ([]eventstore.StoredEvent, error) {
	if fromVersion < 0 {
		fromVersion = 0
	}
	binStreamID, err := streamID.MarshalBinary()
	if err != nil {
		return nil, err
	}
	// #nosec G201 -- tableName is validated in the constructor.
	selectStmt := fmt.Sprintf(
		"SELECT id, event_id, stream_id, stream_version, event_type, payload, occurred_at, metadata FROM %s WHERE stream_id = ? AND stream_version >= ? ORDER BY stream_version ASC LIMIT ?",
		s.tableName)
	rows, err := s.db.QueryContext(ctx, selectStmt, binStreamID, fromVersion, limit)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("failed to close rows in ReadStream", "error", err)
		}
	}()
	return s.transformToStoredEvents(rows)
}

// CleanUpEvents removes the given events from the store by their IncrementID.
func (s *EventStore) CleanUpEvents(ctx context.Context, storedEvents []eventstore.StoredEvent) error {
	if len(storedEvents) == 0 {
		return nil
	}
	valuePlaceholder := make([]string, len(storedEvents))
	valueArgs := make([]interface{}, len(storedEvents))
	for i := range len(storedEvents) {
		valuePlaceholder[i] = "?"
		valueArgs[i] = storedEvents[i].IncrementID
	}
	// #nosec G201 -- tableName is validated in the constructor.
	sqlStmt := fmt.Sprintf("DELETE FROM %s WHERE id IN (%s)", s.tableName, strings.Join(valuePlaceholder, ","))
	_, err := s.db.ExecContext(ctx, sqlStmt, valueArgs...)
	return err
}

// CleanUpToIncluding removes all events whose IncrementID is less than or
// equal to incrementID. The event with IncrementID == incrementID, if any, is
// also removed.
func (s *EventStore) CleanUpToIncluding(ctx context.Context, incrementID int64) error {
	// #nosec G201 -- tableName is validated in the constructor.
	sqlStmt := fmt.Sprintf("DELETE FROM %s WHERE id <= ?", s.tableName)
	_, err := s.db.ExecContext(ctx, sqlStmt, incrementID)
	return err
}

// HasUncommittedID checks if any of the provided IDs exist in the table with read-uncommitted isolation.
func (s *EventStore) HasUncommittedID(ctx context.Context, lowerBound, upperBound int64) (bool, error) {
	hasUncommittedID := false

	err := WithTransaction(ctx, s.db, func(tx *sql.Tx) error {
		// #nosec G201 -- tableName is validated in the constructor.
		query := fmt.Sprintf("SELECT 1 FROM %s WHERE id >= ? AND id <= ? LIMIT 1", s.tableName)

		var dummy int
		err := tx.QueryRowContext(ctx, query, lowerBound, upperBound).Scan(&dummy)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}

		// Found at least one entry; we can stop early
		hasUncommittedID = true
		slog.Debug("Found uncommitted ID in range", "lowerBound", lowerBound, "upperBound", upperBound)
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelReadUncommitted})

	return hasUncommittedID, err
}

// transformToStoredEvents scans the query rows into eventstore.StoredEvents.
func (s *EventStore) transformToStoredEvents(rows *sql.Rows) ([]eventstore.StoredEvent, error) {
	var events []eventstore.StoredEvent
	for rows.Next() {
		var (
			id            int64
			eventID       []byte
			streamID      []byte
			streamVersion sql.NullInt64
			eventPayload  string
			eventType     string
			occurredAt    string
			metadataJSON  sql.NullString
		)
		err := rows.Scan(&id, &eventID, &streamID, &streamVersion, &eventType, &eventPayload, &occurredAt, &metadataJSON)
		if err != nil {
			return nil, err
		}

		// Written in UTC (see Append); read back in UTC. time.DateTime accepts
		// an optional fractional second, so both DATETIME and DATETIME(6)
		// round-trip.
		occurredOnTime, err := time.ParseInLocation(time.DateTime, occurredAt, time.UTC)
		if err != nil {
			return nil, err
		}

		uuidEventID := uuid.UUID{}
		if err := uuidEventID.UnmarshalBinary(eventID); err != nil {
			return nil, err
		}

		uuidStreamID := uuid.UUID{}
		if err := uuidStreamID.UnmarshalBinary(streamID); err != nil {
			return nil, err
		}

		var metadata eventstore.Metadata
		if metadataJSON.Valid && metadataJSON.String != "" {
			if err := json.Unmarshal([]byte(metadataJSON.String), &metadata); err != nil {
				return nil, err
			}
		}

		// NULL stream_version marks an unversioned event (plain Append path);
		// -1 is the documented sentinel on StoredEvent.
		version := -1
		if streamVersion.Valid {
			version = int(streamVersion.Int64)
		}

		events = append(events, eventstore.StoredEvent{
			IncrementID:   id,
			ID:            uuidEventID,
			StreamID:      uuidStreamID,
			StreamVersion: version,
			EventType:     eventType,
			Payload:       eventPayload,
			OccurredAt:    occurredOnTime,
			Metadata:      metadata,
		})
	}
	// rows.Next() returns false on both end-of-set and error; check Err so a
	// truncated result isn't mistaken for a complete one.
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

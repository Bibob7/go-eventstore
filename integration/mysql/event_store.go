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

	"github.com/gofrs/uuid/v5"

	"github.com/Bibob7/go-eventstore"
)

type EventStore struct {
	db        *sql.DB
	tableName string
}

// NewEventStore constructs an EventStore bound to the given database and
// table. The table name must be a valid SQL identifier; otherwise this
// function panics.
//
// The returned value satisfies eventstore.Store (plain append-only log),
// eventstore.StreamStore (optimistic-concurrency appends with a per-stream
// expectedVersion), eventstore.StreamReader (single-stream reads), and
// eventstore.StreamVersionReader (cheap latest-version lookup). Pick the
// interface that matches your use case and depend on it.
func NewEventStore(db *sql.DB, tableName string) *EventStore {
	mustValidateIdentifier("tableName", tableName)
	return &EventStore{
		db:        db,
		tableName: tableName,
	}
}

// Append persists one or more domain events to the event store, optionally
// using a transaction if present in the context. It is the plain
// "append-only log" path: events are inserted in the order received and
// the store does not enforce any per-stream ordering or uniqueness of
// stream_version. Use this for outbox / projection workloads.
//
// For the aggregate-style Load → Decide → Save pattern with optimistic
// concurrency control per stream, use AppendWithExpectedVersion
// (StreamStore interface) instead.
//
// Returns an error if event marshaling fails or the underlying SQL
// execution fails.
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

// AppendWithExpectedVersion atomically appends events to the stream
// identified by streamID and verifies that the stream's current head
// equals expectedVersion before any insert happens. On success the
// events are persisted with consecutive stream versions starting at
// expectedVersion + 1. It satisfies the eventstore.StreamStore interface.
//
// expectedVersion semantics:
//   - -1: the stream MUST be empty. Use this on the create path.
//   - N ≥ 0: the stream's current head MUST be exactly N. This is the
//     case after replaying N+1 events during Load.
//
// All events in the batch MUST have StreamID() == streamID; otherwise a
// *StreamVersionConflictError is returned (treating a mismatched event
// as a concurrency violation keeps the contract simple and surfaces the
// bug at the point of failure).
//
// The check and the insert run inside the same transaction (caller-
// supplied via WithTx/GetTx, or store-owned), so concurrent appends
// to the same stream see either the pre-state or the post-state, never
// an interleaving. A version mismatch is reported as a
// *StreamVersionConflictError wrapping ErrStreamVersionConflict; the
// transaction is rolled back.
func (s *EventStore) AppendWithExpectedVersion(
	ctx context.Context,
	streamID uuid.UUID,
	expectedVersion int,
	domainEvents ...eventstore.DomainEvent,
) error {
	if expectedVersion < -1 {
		return &eventstore.StreamVersionConflictError{
			StreamID: streamID,
			EventID:  uuid.Nil,
			Expected: -1,
			Got:      expectedVersion,
		}
	}
	if len(domainEvents) == 0 {
		return nil
	}
	// Every event in the batch must belong to the same stream; this is
	// part of the StreamStore contract. A mismatch is reported as a
	// conflict (it is almost certainly a bug, but reporting it as a
	// concurrency violation keeps the failure path uniform).
	for i, ev := range domainEvents {
		if ev.StreamID() != streamID {
			return &eventstore.StreamVersionConflictError{
				StreamID: streamID,
				EventID:  ev.ID(),
				Expected: expectedVersion + 1 + i,
				Got:      -1, // signals "wrong stream for this batch"
			}
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

	if tx, exists := GetTx(ctx); exists {
		return run(tx)
	}
	return WithTransaction(ctx, s.db, run, nil)
}

// currentStreamVersion returns the highest stream_version persisted for
// streamID, or -1 if the stream is empty. Runs inside the caller's
// transaction so the read and the subsequent insert are serialized
// against concurrent writers.
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

// insertEvents builds and executes the multi-row INSERT for the batch.
// When versions is non-nil it must have the same length as domainEvents
// and the i-th version is assigned to the i-th event (StreamStore path).
// When versions is nil, every row gets stream_version = 0 (Store path —
// the plain append-only log that does not enforce per-stream ordering).
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
			valuesArgs[j+2] = 0
		}
		valuesArgs[j+3] = domainEvent.EventType()
		valuesArgs[j+4] = eventPayloadJsonString
		// Always persist in UTC so reads are timezone-stable, independent of
		// the producer's local timezone or driver configuration.
		valuesArgs[j+5] = domainEvent.OccurredAt().UTC().Format(time.DateTime)
		// Metadata: nil and empty maps both map to SQL NULL so we don't pay
		// for a JSON round-trip on every row when no metadata is attached.
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

// FetchBatchOfEvents fetches a batch of events from the event store starting with the smallest incrementID.
// Unlike FetchBatchOfEventsSince, no gap detection is applied because this method is intended for
// transient relay usage where processed events are deleted, making ID gaps expected and harmless.
func (s *EventStore) FetchBatchOfEvents(ctx context.Context, limit int) ([]eventstore.StoredEvent, error) {
	return s.fetchBatchOfEvents(ctx, -1, limit)
}

// FetchBatchOfEventsSince fetches a batch of events from the event store since the last incrementID.
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

// LatestStreamVersion returns the highest StreamVersion persisted for
// streamID, or -1 if no events exist for that stream. It satisfies the
// eventstore.StreamVersionReader interface and issues a single aggregate
// query (SELECT MAX(stream_version) WHERE stream_id = ?), so it is the
// cheap alternative to ReadStream whenever the caller only needs the
// position — e.g. as a pre-check before Append to detect concurrent
// writers without deserializing the full history, or to compute
// fromVersion = lastVersion + 1 when resuming from a snapshot.
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
		// No row matched — the stream is empty. -1 is the documented
		// sentinel for "fresh stream" so callers can compute the next
		// expected version as last + 1.
		return -1, nil
	}
	return int(maxVersion.Int64), nil
}

// ReadStream returns up to limit events for the given stream, ordered by
// StreamVersion ascending, starting at fromVersion inclusive. It satisfies
// the eventstore.StreamReader interface.
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

// CleanUpEvents removes a list of stored events from the event store based on their IncrementID values.
// It constructs and executes an SQL DELETE statement to clean up the specified events.
// Returns an error if there is an issue during SQL execution.
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

// transformToStoredEvents scans SQL query result rows and converts them into a slice of eventstore.StoredEvents.
// Returns an error if row scanning, data parsing, or UUID unmarshalling fails.
func (s *EventStore) transformToStoredEvents(rows *sql.Rows) ([]eventstore.StoredEvent, error) {
	var events []eventstore.StoredEvent
	for rows.Next() {
		var (
			id            int64
			eventID       []byte
			streamID      []byte
			streamVersion int
			eventPayload  string
			eventType     string
			occurredAt    string
			metadataJSON  sql.NullString
		)
		err := rows.Scan(&id, &eventID, &streamID, &streamVersion, &eventType, &eventPayload, &occurredAt, &metadataJSON)
		if err != nil {
			return nil, err
		}

		// Values are written in UTC (see Append), so read them back in UTC
		// to guarantee a timezone-stable round-trip.
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

		events = append(events, eventstore.StoredEvent{
			IncrementID:   id,
			ID:            uuidEventID,
			StreamID:      uuidStreamID,
			StreamVersion: streamVersion,
			EventType:     eventType,
			Payload:       eventPayload,
			OccurredAt:    occurredOnTime,
			Metadata:      metadata,
		})
	}
	return events, nil
}

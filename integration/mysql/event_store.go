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
	"github.com/Bibob7/go-eventstore/filter"
)

type EventStore struct {
	db        *sql.DB
	tableName string
}

// NewEventStore constructs an EventStore bound to the given database and table.
// The table name must be a valid SQL identifier; otherwise this function panics.
func NewEventStore(db *sql.DB, tableName string) *EventStore {
	mustValidateIdentifier("tableName", tableName)
	return &EventStore{
		db:        db,
		tableName: tableName,
	}
}

// Append adds one or more domain events to the event store, optionally using a transaction if present in the context.
// It converts events to their corresponding database representation and executes an SQL insert statement.
// Returns an error if event marshaling fails or the execution of the SQL insert statement is unsuccessful.
func (s *EventStore) Append(ctx context.Context, domainEvents ...eventstore.DomainEvent) error {
	argsNum := 5
	if len(domainEvents) == 0 {
		return nil
	}

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

		valuesStrings[i] = "(?, ?, ?, ?, ?)"
		j := i * argsNum
		binaryId, err := domainEvent.ID().MarshalBinary()
		if err != nil {
			return err
		}

		binaryAggregateId, err := domainEvent.AggregateID().MarshalBinary()
		if err != nil {
			return err
		}

		valuesArgs[j] = binaryId
		valuesArgs[j+1] = binaryAggregateId
		valuesArgs[j+2] = domainEvent.EventType()
		valuesArgs[j+3] = eventPayloadJsonString
		// Always persist in UTC so reads are timezone-stable, independent of
		// the producer's local timezone or driver configuration.
		valuesArgs[j+4] = domainEvent.OccurredAt().UTC().Format(time.DateTime)
	}

	// #nosec G201 -- tableName is validated in the constructor.
	sqlStmt := fmt.Sprintf("INSERT INTO %s (event_id, aggregate_id, event_type, payload, occurred_at) VALUES %s", s.tableName, strings.Join(valuesStrings, ","))

	if tx, exists := GetTx(ctx); exists {
		slog.Debug("Appending domainEvents to DomainEvent eventStore in transaction")
		_, err := tx.ExecContext(ctx, sqlStmt, valuesArgs...)
		return err
	}
	slog.Debug("Appending domainEvents to DomainEvent eventStore without transaction")
	_, err := s.db.ExecContext(ctx, sqlStmt, valuesArgs...)
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
	return filter.NewUntilGapEventFilter(lastIncrementID, s).Execute(ctx, storedEvents)
}

func (s *EventStore) fetchBatchOfEvents(ctx context.Context, lastIncrementID int64, limit int) ([]eventstore.StoredEvent, error) {
	// #nosec G201 -- tableName is validated in the constructor.
	selectStmt := fmt.Sprintf(
		"SELECT id, event_id, aggregate_id, event_type, payload, occurred_at FROM %s",
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

// CleanUpEvents removes a list of stored events from the event store based on their IncrementID values.
// It constructs and executes an SQL DELETE statement to clean up the specified events.
// Returns an error if there is an issue during SQL statement execution.
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
			id           int64
			eventID      []byte
			aggregateID  []byte
			eventPayload string
			eventType    string
			occurredAt   string
		)
		err := rows.Scan(&id, &eventID, &aggregateID, &eventType, &eventPayload, &occurredAt)
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

		uuidAggregateID := uuid.UUID{}
		if err := uuidAggregateID.UnmarshalBinary(aggregateID); err != nil {
			return nil, err
		}

		events = append(events, eventstore.StoredEvent{
			IncrementID: id,
			ID:          uuidEventID,
			EntityID:    uuidAggregateID,
			EventType:   eventType,
			Payload:     eventPayload,
			OccurredAt:  occurredOnTime,
		})
	}
	return events, nil
}

package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/Bibob7/go-eventstore"
	"github.com/Bibob7/go-eventstore/filter"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/gofrs/uuid/v5"
)

type EventStore struct {
	db        *sql.DB
	tableName string
}

func NewEventStore(db *sql.DB, tableName string) *EventStore {
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
		valuesArgs[j+4] = domainEvent.OccurredAt().Format(time.DateTime)
	}

	// #nosec G201
	sqlStmt := fmt.Sprintf("INSERT INTO %s (event_id, aggregate_id, event_type, payload, occurred_at) VALUES %s", s.tableName, strings.Join(valuesStrings, ","))

	var err error
	tx, exists := GetTx(ctx)
	if exists {
		slog.Debug("Appending domainEvents to DomainEvent eventStore in transaction")
		_, err = tx.Exec(sqlStmt, valuesArgs...)
	} else {
		slog.Debug("Appending domainEvents to DomainEvent eventStore without transaction")
		_, err = s.db.ExecContext(ctx, sqlStmt, valuesArgs...)
	}
	if err != nil {
		slog.Error("Error appending domainEvents to DomainEvent eventStore", "error", err)
	}
	return err
}

// FetchBatchOfEvents fetches a batch of events from the event store starting with the smallest incrementID.
// Providing eventIDs is optional.
func (s *EventStore) FetchBatchOfEvents(ctx context.Context, limit int) ([]eventstore.StoredEvent, error) {
	return s.FetchBatchOfEventsSince(ctx, -1, limit)
}

// FetchBatchOfEventsSince fetches a batch of events from the event store since the last incrementID.
// Providing eventIDs is optional.
func (s *EventStore) FetchBatchOfEventsSince(ctx context.Context, lastIncrementID int64, limit int) ([]eventstore.StoredEvent, error) {
	// #nosec G201
	selectStmt := fmt.Sprintf(
		"SELECT id, event_id, aggregate_id, event_type, payload, occurred_at FROM %s WHERE id > ? ORDER BY id ASC LIMIT ?",
		s.tableName)
	slog.Debug("Fetching events from eventStore", "lastIncrementID", lastIncrementID, "limit", limit, "selectStmt", selectStmt)
	rows, err := s.db.QueryContext(ctx, selectStmt, lastIncrementID, limit)
	if err != nil {
		slog.Error("Error fetching events from eventStore", "error", err)
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("failed to close rows", "error", err)
		}
	}()
	storedEvents, err := s.transformToStoredEvents(rows)
	if err != nil {
		return nil, err
	}
	return filter.NewUntilGapEventFilter(lastIncrementID, s).Execute(ctx, storedEvents)
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
		valueArgs[i] = fmt.Sprintf("%d", storedEvents[i].IncrementID)
	}
	// #nosec G201
	sqlStmt := fmt.Sprintf("DELETE FROM %s WHERE id IN (%s)", s.tableName, strings.Join(valuePlaceholder, ","))
	_, err := s.db.ExecContext(ctx, sqlStmt, valueArgs...)
	if err != nil {
		slog.Error("Error cleaning up storedEvents from DomainEvent eventStore", "error", err)
	}
	return err
}

// HasUncommittedID checks if any of the provided IDs exist in the table with read-uncommitted isolation.
func (s *EventStore) HasUncommittedID(ctx context.Context, lowerBound, upperBound int64) (bool, error) {
	hasUncommittedID := false

	err := WithTransaction(ctx, s.db, func(tx *sql.Tx) error {
		// #nosec G201
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
			aggregateId  string
			eventPayload string
			eventType    string
			occurredAt   string
		)
		err := rows.Scan(&id, &eventID, &aggregateId, &eventType, &eventPayload, &occurredAt)
		if err != nil {
			return nil, err
		}

		occurredOnTime, err := time.Parse(time.DateTime, occurredAt)
		if err != nil {
			return nil, err
		}

		uuidEventID := uuid.UUID{}
		err = uuidEventID.UnmarshalBinary(eventID)
		if err != nil {
			return nil, err
		}

		events = append(events, eventstore.StoredEvent{
			IncrementID: id,
			ID:          uuidEventID,
			EntityID:    uuid.FromStringOrNil(aggregateId),
			EventType:   eventType,
			Payload:     eventPayload,
			OccurredAt:  occurredOnTime,
		})
	}
	return events, nil
}

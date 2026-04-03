package eventstore

import (
	"context"
)

type Store interface {
	Append(ctx context.Context, events ...DomainEvent) error
}

type CleanUpStore interface {
	Store
	FetchBatchOfEvents(ctx context.Context, limit int) ([]StoredEvent, error)
	CleanUpEvents(ctx context.Context, events []StoredEvent) error
}

type PointerStore interface {
	Store
	FetchBatchOfEventsSince(ctx context.Context, lastIncrementID int64, limit int) ([]StoredEvent, error)
}

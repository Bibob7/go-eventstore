package eventstore

import "context"

type Filter interface {
	Execute(ctx context.Context, storedEvents []StoredEvent) ([]StoredEvent, error)
}

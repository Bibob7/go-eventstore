package eventstore

import "context"

// IncrementIDStore persists the last successfully processed IncrementID per relay.
// It is used to resume event processing after a restart without re-processing events.
type IncrementIDStore interface {
	// SetIncrementID stores the last processed IncrementID for the given relay.
	// Implementations must reject the write with ErrIncrementIDConflict when the
	// currently stored value differs from expectedPreviousID.
	SetIncrementID(ctx context.Context, relayName string, expectedPreviousID int64, incrementID int64) error
	// GetIncrementID returns the last processed IncrementID for the given relay,
	// or 0 if no position has been recorded yet.
	GetIncrementID(ctx context.Context, relayName string) (int64, error)
}

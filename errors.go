package eventstore

import (
	"errors"
	"strconv"

	"github.com/gofrs/uuid/v5"
)

// ErrStreamVersionConflict is returned by StreamStore.AppendWithExpectedVersion
// when the stream's head does not match the caller's expectedVersion, or a
// concurrent writer wins the race. Unwrap it to a *StreamVersionConflictError
// for the conflicting stream and event; callers should reload and retry.
var ErrStreamVersionConflict = errors.New("event store: stream version conflict")

// ErrInvalidStreamAppend is returned by StreamStore.AppendWithExpectedVersion
// when the call is malformed: an event belongs to a different stream, or
// expectedVersion is below -1. These are programming errors, reported as a
// distinct sentinel so retry loops do not catch them.
var ErrInvalidStreamAppend = errors.New("event store: invalid stream append")

// StreamVersionConflictError carries the stream and event that triggered an
// ErrStreamVersionConflict, which it wraps.
type StreamVersionConflictError struct {
	// StreamID is the stream the conflicting event belongs to.
	StreamID uuid.UUID
	// EventID is the event that failed the version check.
	EventID uuid.UUID
	// Expected is the stream version the caller expected to be the current head.
	Expected int
	// Got is the actual stream head (highest StreamVersion) observed by the store.
	Got int
}

func (e *StreamVersionConflictError) Error() string {
	return "event store: stream " + e.StreamID.String() +
		": event " + e.EventID.String() + " has version " +
		strconv.Itoa(e.Got) + ", expected " + strconv.Itoa(e.Expected)
}

// Unwrap makes errors.Is(err, ErrStreamVersionConflict) work.
func (e *StreamVersionConflictError) Unwrap() error { return ErrStreamVersionConflict }

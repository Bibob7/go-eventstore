package eventstore

import (
	"errors"
	"strconv"

	"github.com/gofrs/uuid/v5"
)

// ErrStreamVersionConflict is returned by StreamStore.AppendWithExpectedVersion
// when the stream's current head does not match the caller's expectedVersion,
// or when a concurrent writer wins the race for the same stream version.
// event can be inspected by unwrapping the error:
//
//	var c *eventstore.StreamVersionConflictError
//	if errors.As(err, &c) {
//	    log.Printf("conflict on stream %s, event %s", c.StreamID, c.EventID)
//	}
//
// Concurrent aggregates can use this signal to reload and retry from the
// freshly observed stream state.
var ErrStreamVersionConflict = errors.New("event store: stream version conflict")

// ErrInvalidStreamAppend is returned by StreamStore.AppendWithExpectedVersion
// when the call itself is malformed: an event in the batch belongs to a
// different stream than the streamID parameter, or expectedVersion is below
// the -1 sentinel. These are programming errors, not concurrency conflicts —
// reloading and retrying (the correct reaction to ErrStreamVersionConflict)
// cannot fix them, so they are reported as a distinct sentinel that retry
// loops must not catch.
var ErrInvalidStreamAppend = errors.New("event store: invalid stream append")

// StreamVersionConflictError provides the concrete stream/event that
// triggered an ErrStreamVersionConflict. The sentinel is the
// `errors.Is`-friendly value; this struct carries the diagnostics.
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

// Error implements the error interface.
func (e *StreamVersionConflictError) Error() string {
	return "event store: stream " + e.StreamID.String() +
		": event " + e.EventID.String() + " has version " +
		strconv.Itoa(e.Got) + ", expected " + strconv.Itoa(e.Expected)
}

// Unwrap makes errors.Is(err, ErrStreamVersionConflict) work.
func (e *StreamVersionConflictError) Unwrap() error { return ErrStreamVersionConflict }

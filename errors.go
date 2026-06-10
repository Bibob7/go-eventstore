package eventstore

import (
	"errors"
	"strconv"

	"github.com/gofrs/uuid/v5"
)

// ErrStreamVersionConflict is returned by Store.Append when one or more
// events in the batch claim a StreamVersion that does not match the next
// expected version for their stream. The exact conflicting stream and
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

// StreamVersionConflictError provides the concrete stream/event that
// triggered an ErrStreamVersionConflict. The sentinel is the
// `errors.Is`-friendly value; this struct carries the diagnostics.
type StreamVersionConflictError struct {
	// StreamID is the stream the conflicting event belongs to.
	StreamID uuid.UUID
	// EventID is the event that failed the version check.
	EventID uuid.UUID
	// Expected is the version the store expected for this event.
	Expected int
	// Got is the version the event claimed.
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

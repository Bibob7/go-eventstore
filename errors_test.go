package eventstore

import (
	"errors"
	"testing"

	"github.com/gofrs/uuid/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStreamVersionConflictError_IsAndUnwrap verifies that the typed error
// satisfies errors.Is(err, ErrStreamVersionConflict) and errors.As(err, &c)
// for diagnostic details.
func TestStreamVersionConflictError_IsAndUnwrap(t *testing.T) {
	streamID := uuid.Must(uuid.NewV4())
	eventID := uuid.Must(uuid.NewV4())

	tests := []struct {
		name string
		err  error
	}{
		{
			name: "direct value",
			err: &StreamVersionConflictError{
				StreamID: streamID,
				EventID:  eventID,
				Expected: 5,
				Got:      3,
			},
		},
		{
			name: "wrapped via fmt.Errorf",
			err: errors.Join(
				errors.New("outer"),
				&StreamVersionConflictError{
					StreamID: streamID,
					EventID:  eventID,
					Expected: 1,
					Got:      0,
				},
			),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.True(t, errors.Is(tc.err, ErrStreamVersionConflict),
				"errors.Is must recognize the sentinel")

			var c *StreamVersionConflictError
			require.True(t, errors.As(tc.err, &c),
				"errors.As must surface the typed conflict")
			assert.Equal(t, streamID, c.StreamID)
			assert.Equal(t, eventID, c.EventID)
		})
	}
}

// TestStreamVersionConflictError_MessageIsInformative pins the error format
// loosely so a regression that drops the diagnostic fields is caught.
func TestStreamVersionConflictError_MessageIsInformative(t *testing.T) {
	streamID := uuid.Must(uuid.NewV4())
	eventID := uuid.Must(uuid.NewV4())
	err := &StreamVersionConflictError{
		StreamID: streamID,
		EventID:  eventID,
		Expected: 5,
		Got:      3,
	}
	msg := err.Error()
	assert.Contains(t, msg, streamID.String())
	assert.Contains(t, msg, eventID.String())
	assert.Contains(t, msg, "3")
	assert.Contains(t, msg, "5")
}

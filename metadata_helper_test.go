package eventstore

import (
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/stretchr/testify/assert"
)

// fixtureEvent is a DomainEvent used only by the metadata-helper tests. It
// embeds BaseEvent to get the default nil Metadata() and supplies the
// remaining DomainEvent methods with throwaway values.
type fixtureEvent struct {
	BaseEvent
}

func (fixtureEvent) ID() uuid.UUID         { return uuid.Nil }
func (fixtureEvent) StreamID() uuid.UUID   { return uuid.Nil }
func (fixtureEvent) EventType() string     { return "fixture" }
func (fixtureEvent) OccurredAt() time.Time { return time.Time{} }

// fixturePointerEvent embeds *BaseEvent to verify that pointer embedding
// also forwards Metadata() correctly.
type fixturePointerEvent struct {
	*BaseEvent
}

func (fixturePointerEvent) ID() uuid.UUID         { return uuid.Nil }
func (fixturePointerEvent) StreamID() uuid.UUID   { return uuid.Nil }
func (fixturePointerEvent) EventType() string     { return "fixture-pointer" }
func (fixturePointerEvent) OccurredAt() time.Time { return time.Time{} }

// TestBaseEvent_DefaultsToNilMetadata verifies that embedding BaseEvent in a
// domain event means the event reports no metadata by default, which the
// store treats as equivalent to an empty map.
func TestBaseEvent_DefaultsToNilMetadata(t *testing.T) {
	tests := []struct {
		name string
		// fn returns a fresh DomainEvent so each table row is independent.
		fn func() DomainEvent
	}{
		{
			name: "struct embedding BaseEvent by value",
			fn:   func() DomainEvent { return fixtureEvent{} },
		},
		{
			name: "struct embedding *BaseEvent by pointer",
			fn:   func() DomainEvent { return fixturePointerEvent{BaseEvent: &BaseEvent{}} },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ev := tc.fn()
			// Compile-time check: the constructed value is a DomainEvent.
			var _ DomainEvent = ev
			// BaseEvent opts the embedding type into MetadataProvider.
			mp, ok := ev.(MetadataProvider)
			assert.True(t, ok, "BaseEvent must satisfy MetadataProvider")
			assert.Nil(t, mp.Metadata(),
				"BaseEvent must report nil metadata by default")
		})
	}
}

// TestMetadata_KeyConstants pins the string values of the reserved keys so
// downstream tools (log aggregators, OpenTelemetry bridges) can rely on them.
func TestMetadata_KeyConstants(t *testing.T) {
	assert.Equal(t, "correlation_id", MetadataKeyCorrelationID)
	assert.Equal(t, "causation_id", MetadataKeyCausationID)
	assert.Equal(t, "trace_id", MetadataKeyTraceID)
}

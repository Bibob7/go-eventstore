package filter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/Bibob7/go-eventstore"
)

type mockGapDetector struct {
	mock.Mock
}

func (m *mockGapDetector) HasUncommittedID(ctx context.Context, lowerBound, upperBound int64) (bool, error) {
	args := m.Called(ctx, lowerBound, upperBound)
	return args.Bool(0), args.Error(1)
}

func eventsWithIDs(ids ...int64) []eventstore.StoredEvent {
	events := make([]eventstore.StoredEvent, len(ids))
	for i, id := range ids {
		uid, _ := uuid.NewV4()
		events[i] = eventstore.StoredEvent{
			IncrementID: id,
			ID:          uid,
			EntityID:    uid,
			EventType:   "test_event",
			Payload:     "{}",
			OccurredAt:  time.Now(),
		}
	}
	return events
}

func extractIDs(events []eventstore.StoredEvent) []int64 {
	ids := make([]int64, len(events))
	for i, e := range events {
		ids[i] = e.IncrementID
	}
	return ids
}

func TestGapDetectionEventFilter_Execute(t *testing.T) {
	sentinel := errors.New("detector error")

	tests := []struct {
		name            string
		lastIncrementID int64
		events          []eventstore.StoredEvent
		setupDetector   func(*mockGapDetector)
		wantIDs         []int64
		wantErr         error
	}{
		{
			name:            "no gaps returns all events",
			lastIncrementID: 0,
			events:          eventsWithIDs(1, 2, 3),
			setupDetector:   func(_ *mockGapDetector) {},
			wantIDs:         []int64{1, 2, 3},
		},
		{
			name:            "empty event list returns empty result",
			lastIncrementID: 0,
			events:          []eventstore.StoredEvent{},
			setupDetector:   func(_ *mockGapDetector) {},
			wantIDs:         []int64{},
		},
		{
			name:            "gap with uncommitted ID stops before gap",
			lastIncrementID: 1,
			events:          eventsWithIDs(2, 3, 5, 6),
			setupDetector: func(d *mockGapDetector) {
				d.On("HasUncommittedID", mock.Anything, int64(4), int64(4)).Return(true, nil)
			},
			wantIDs: []int64{2, 3},
		},
		{
			name:            "gap with committed ID continues past gap",
			lastIncrementID: 1,
			events:          eventsWithIDs(3, 4, 5),
			setupDetector: func(d *mockGapDetector) {
				d.On("HasUncommittedID", mock.Anything, int64(2), int64(2)).Return(false, nil)
			},
			wantIDs: []int64{3, 4, 5},
		},
		{
			name:            "large gap with committed IDs continues past gap",
			lastIncrementID: 0,
			events:          eventsWithIDs(1, 2, 5000),
			setupDetector: func(d *mockGapDetector) {
				d.On("HasUncommittedID", mock.Anything, int64(3), int64(4999)).Return(false, nil)
			},
			wantIDs: []int64{1, 2, 5000},
		},
		{
			name:            "detector error propagates and returns nil",
			lastIncrementID: 1,
			events:          eventsWithIDs(3),
			setupDetector: func(d *mockGapDetector) {
				d.On("HasUncommittedID", mock.Anything, int64(2), int64(2)).Return(false, sentinel)
			},
			wantErr: sentinel,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			detector := &mockGapDetector{}
			tc.setupDetector(detector)

			f := NewUntilGapEventFilter(tc.lastIncrementID, detector)
			result, err := f.Execute(context.Background(), tc.events)

			if tc.wantErr != nil {
				assert.ErrorIs(t, err, tc.wantErr)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantIDs, extractIDs(result))
			}
			detector.AssertExpectations(t)
		})
	}
}

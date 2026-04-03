package filter

import (
	"context"
	"slices"
	"testing"
	"time"

	"eventstore"

	"github.com/gofrs/uuid/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockGapDetector struct {
	mock.Mock
}

func (m *mockGapDetector) HasUncommittedID(ctx context.Context, lowerBound, upperBound int64) (bool, error) {
	args := m.Called(ctx, lowerBound, upperBound)
	return args.Bool(0), args.Error(1)
}

func createTestEvents(startID int64, count int, skipIDs []int64) []eventstore.StoredEvent {
	var events []eventstore.StoredEvent
	for incrementID := startID; len(events) < count; incrementID++ {
		if slices.Contains(skipIDs, incrementID) {
			continue
		}
		id, _ := uuid.NewV4()
		events = append(events, eventstore.StoredEvent{
			IncrementID: incrementID,
			ID:          id,
			EntityID:    id,
			EventType:   "test_event",
			Payload:     "{}",
			OccurredAt:  time.Now(),
		})
	}
	return events
}

func TestGapDetectionEventFilter_Execute(t *testing.T) {
	ctx := context.Background()

	t.Run("no gaps in sequence", func(t *testing.T) {
		detector := &mockGapDetector{}
		filter := NewUntilGapEventFilter(0, detector)

		events := createTestEvents(1, 3, nil)

		result, err := filter.Execute(ctx, events)

		assert.NoError(t, err)
		assert.Len(t, result, 3)
		assert.Equal(t, events, result)
		detector.AssertNumberOfCalls(t, "HasUncommittedID", 0)
	})

	t.Run("gap with uncommitted UserID", func(t *testing.T) {
		detector := &mockGapDetector{}
		filter := NewUntilGapEventFilter(1, detector)

		events := createTestEvents(2, 4, []int64{4})
		detector.On("HasUncommittedID", ctx, int64(4), int64(4)).Return(true, nil)

		result, err := filter.Execute(ctx, events)

		assert.NoError(t, err)
		assert.Equal(t, int64(3), result[len(result)-1].IncrementID)
		detector.AssertExpectations(t)
	})

	t.Run("gap with committed UserID", func(t *testing.T) {
		detector := &mockGapDetector{}
		filter := NewUntilGapEventFilter(1, detector)

		events := createTestEvents(3, 3, nil)
		detector.On("HasUncommittedID", ctx, int64(2), int64(2)).Return(false, nil)

		result, err := filter.Execute(ctx, events)

		assert.NoError(t, err)
		assert.Len(t, result, 3)
		assert.Equal(t, events, result)
		assert.Equal(t, int64(5), result[len(result)-1].IncrementID)
		detector.AssertExpectations(t)
	})

	t.Run("empty events list", func(t *testing.T) {
		detector := &mockGapDetector{}
		filter := NewUntilGapEventFilter(0, detector)

		result, err := filter.Execute(ctx, []eventstore.StoredEvent{})

		assert.NoError(t, err)
		assert.Empty(t, result)
		detector.AssertNotCalled(t, "HasUncommittedID")
	})

	t.Run("detector error", func(t *testing.T) {
		detector := &mockGapDetector{}
		filter := NewUntilGapEventFilter(1, detector)

		events := createTestEvents(3, 1, nil)
		expectedError := assert.AnError
		detector.On("HasUncommittedID", ctx, int64(2), int64(2)).Return(false, expectedError)

		result, err := filter.Execute(ctx, events)

		assert.Error(t, err)
		assert.ErrorIs(t, err, expectedError)
		assert.Nil(t, result)
		detector.AssertExpectations(t)
	})
}

package eventstore

import (
	"context"
)

type GapDetector interface {
	HasUncommittedID(ctx context.Context, gapLow, gapHigh int64) (bool, error)
}

type untilGapEventFilter struct {
	lastIncrementID int64
	gapDetector     GapDetector
}

func NewUntilGapEventFilter(lastIncrementID int64, gapDetector GapDetector) Filter {
	return &untilGapEventFilter{
		lastIncrementID: lastIncrementID,
		gapDetector:     gapDetector,
	}
}

// Execute returns the leading run of events with no gap in their IncrementID,
// stopping at the first gap that the GapDetector reports as still uncommitted.
func (f *untilGapEventFilter) Execute(ctx context.Context, storedEvents []StoredEvent) ([]StoredEvent, error) {
	expectedIncrementID := f.lastIncrementID
	var filteredEvents []StoredEvent

	for i := 0; i < len(storedEvents); i++ {
		storedEvent := storedEvents[i]
		expectedIncrementID++

		if storedEvent.IncrementID > expectedIncrementID {
			hasUncommitted, err := f.gapDetector.HasUncommittedID(ctx, expectedIncrementID, storedEvent.IncrementID-1)
			if err != nil {
				return nil, err
			}

			if hasUncommitted {
				return filteredEvents, nil
			}
			expectedIncrementID = storedEvent.IncrementID
		}
		filteredEvents = append(filteredEvents, storedEvent)
	}
	return filteredEvents, nil
}

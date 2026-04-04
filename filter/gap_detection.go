package filter

import (
	"context"

	"github.com/Bibob7/go-eventstore"
)

type GapDetector interface {
	HasUncommittedID(ctx context.Context, gapLow, gapHigh int64) (bool, error)
}

type untilGapEventFilter struct {
	lastIncrementID int64
	gapDetector     GapDetector
}

func NewUntilGapEventFilter(lastIncrementID int64, gapDetector GapDetector) eventstore.Filter {
	return &untilGapEventFilter{
		lastIncrementID: lastIncrementID,
		gapDetector:     gapDetector,
	}
}

// Execute filters stored events based on increment ID gaps and detects uncommitted IDs using a gap detector contextually.
func (f *untilGapEventFilter) Execute(ctx context.Context, storedEvents []eventstore.StoredEvent) ([]eventstore.StoredEvent, error) {
	expectedIncrementID := f.lastIncrementID
	var filteredEvents []eventstore.StoredEvent

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

package eventstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
)

// fakeStreamReader serves a fixed, version-ordered stream and honours the
// fromVersion/limit contract of StreamReader so ReadStreamAll's paging can
// be exercised. It records how many times ReadStream was called.
type fakeStreamReader struct {
	events []StoredEvent
	err    error
	calls  int
}

func (f *fakeStreamReader) ReadStream(_ context.Context, _ uuid.UUID, fromVersion, limit int) ([]StoredEvent, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.calls++
	var out []StoredEvent
	for _, e := range f.events {
		if e.StreamVersion >= fromVersion {
			out = append(out, e)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

// streamEvents builds a gapless stream of n events with StreamVersion
// 0..n-1, mirroring how AppendWithExpectedVersion assigns versions.
func streamEvents(n int) []StoredEvent {
	events := make([]StoredEvent, n)
	for i := range events {
		id, _ := uuid.NewV4()
		events[i] = StoredEvent{
			ID:            id,
			StreamID:      id,
			IncrementID:   int64(i + 1),
			StreamVersion: i,
			EventType:     "test-event",
			OccurredAt:    time.Now(),
		}
	}
	return events
}

func TestReadStreamAll(t *testing.T) {
	sentinel := errors.New("read error")

	tests := []struct {
		name         string
		streamLen    int
		fromVersion  int
		pageSize     int
		err          error
		wantVersions []int
		wantCalls    int
		wantErr      error
	}{
		{
			name:         "empty stream returns nothing in one call",
			streamLen:    0,
			pageSize:     10,
			wantVersions: nil,
			wantCalls:    1,
		},
		{
			name:         "stream shorter than page size fits in one call",
			streamLen:    3,
			pageSize:     10,
			wantVersions: []int{0, 1, 2},
			wantCalls:    1,
		},
		{
			name:         "stream longer than page size is paged without truncation",
			streamLen:    25,
			pageSize:     10,
			wantVersions: rangeInts(0, 25),
			wantCalls:    3, // 10 + 10 + 5
		},
		{
			name:         "stream length an exact multiple of page size needs a trailing empty page",
			streamLen:    20,
			pageSize:     10,
			wantVersions: rangeInts(0, 20),
			wantCalls:    3, // 10 + 10 + 0
		},
		{
			name:         "fromVersion skips the prefix",
			streamLen:    10,
			fromVersion:  7,
			pageSize:     10,
			wantVersions: []int{7, 8, 9},
			wantCalls:    1,
		},
		{
			name:         "negative fromVersion is treated as zero",
			streamLen:    3,
			fromVersion:  -5,
			pageSize:     10,
			wantVersions: []int{0, 1, 2},
			wantCalls:    1,
		},
		{
			name:         "non-positive page size falls back to default batch size",
			streamLen:    3,
			pageSize:     0,
			wantVersions: []int{0, 1, 2},
			wantCalls:    1,
		},
		{
			name:      "read error propagates with no partial result",
			streamLen: 5,
			pageSize:  2,
			err:       sentinel,
			wantErr:   sentinel,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := &fakeStreamReader{events: streamEvents(tc.streamLen), err: tc.err}
			streamID := uuid.Must(uuid.NewV4())

			got, err := ReadStreamAll(context.Background(), reader, streamID, tc.fromVersion, tc.pageSize)

			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("expected error %v, got %v", tc.wantErr, err)
				}
				if got != nil {
					t.Errorf("expected nil result on error, got %d events", len(got))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotVersions := versionsOf(got); !equalInts(gotVersions, tc.wantVersions) {
				t.Errorf("expected versions %v, got %v", tc.wantVersions, gotVersions)
			}
			if tc.wantCalls != 0 && reader.calls != tc.wantCalls {
				t.Errorf("expected %d ReadStream call(s), got %d", tc.wantCalls, reader.calls)
			}
		})
	}
}

func versionsOf(events []StoredEvent) []int {
	if len(events) == 0 {
		return nil
	}
	out := make([]int, len(events))
	for i, e := range events {
		out[i] = e.StreamVersion
	}
	return out
}

func rangeInts(start, end int) []int {
	out := make([]int, 0, end-start)
	for i := start; i < end; i++ {
		out = append(out, i)
	}
	return out
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

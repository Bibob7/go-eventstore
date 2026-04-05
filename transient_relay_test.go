package eventstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
)

type mockCleanUpStore struct {
	events     []StoredEvent
	fetchErr   error
	cleanUpErr error
	cleanedUp  []StoredEvent
}

func (m *mockCleanUpStore) FetchBatchOfEvents(_ context.Context, limit int) ([]StoredEvent, error) {
	if m.fetchErr != nil {
		return nil, m.fetchErr
	}
	if len(m.events) > limit {
		return m.events[:limit], nil
	}
	return m.events, nil
}

func (m *mockCleanUpStore) CleanUpEvents(_ context.Context, events []StoredEvent) error {
	if m.cleanUpErr != nil {
		return m.cleanUpErr
	}
	m.cleanedUp = append(m.cleanedUp, events...)
	return nil
}

func newStoredEvent(incrementID int64) StoredEvent {
	id, _ := uuid.NewV4()
	return StoredEvent{ID: id, EntityID: id, IncrementID: incrementID, EventType: "test-event", OccurredAt: time.Now()}
}

func TestTransientRelay_Name(t *testing.T) {
	tests := []struct {
		name      string
		relayName string
	}{
		{name: "returns configured name", relayName: "my-relay"},
		{name: "returns empty name", relayName: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			relay := NewTransientRelay(tc.relayName, &mockCleanUpStore{})
			if relay.Name() != tc.relayName {
				t.Fatalf("expected name %q, got %q", tc.relayName, relay.Name())
			}
		})
	}
}

func TestTransientRelay_Run(t *testing.T) {
	tests := []struct {
		name          string
		events        []StoredEvent
		fetchErr      error
		cleanUpErr    error
		handlerErr    error
		secondHandler bool // adds a second handler to verify ordering / abort behaviour
		opts          []RelayOption
		wantErr       bool
		wantHandled   int
		wantCleanedUp int
		wantH2Called  bool
	}{
		{
			name:          "no events",
			wantHandled:   0,
			wantCleanedUp: 0,
		},
		{
			name:          "handles and cleans up all events",
			events:        []StoredEvent{newStoredEvent(1), newStoredEvent(2), newStoredEvent(3)},
			wantHandled:   3,
			wantCleanedUp: 3,
		},
		{
			name:          "respects batch size",
			events:        []StoredEvent{newStoredEvent(1), newStoredEvent(2), newStoredEvent(3)},
			opts:          []RelayOption{WithBatchSize(2)},
			wantHandled:   2,
			wantCleanedUp: 2,
		},
		{
			name:          "fetch error propagates",
			fetchErr:      errors.New("fetch error"),
			wantErr:       true,
			wantHandled:   0,
			wantCleanedUp: 0,
		},
		{
			name:          "handler error prevents clean up",
			events:        []StoredEvent{newStoredEvent(1)},
			handlerErr:    errors.New("handler error"),
			wantErr:       true,
			wantHandled:   1,
			wantCleanedUp: 0,
		},
		{
			name:          "ErrEventNotReadyToProcess prevents clean up",
			events:        []StoredEvent{newStoredEvent(1)},
			handlerErr:    ErrEventNotReadyToProcess,
			wantErr:       true,
			wantHandled:   1,
			wantCleanedUp: 0,
		},
		{
			name:          "clean up error propagates",
			events:        []StoredEvent{newStoredEvent(1)},
			cleanUpErr:    errors.New("cleanup error"),
			wantErr:       true,
			wantHandled:   1,
			wantCleanedUp: 0,
		},
		{
			name:          "all handlers called on success",
			events:        []StoredEvent{newStoredEvent(1)},
			secondHandler: true,
			wantHandled:   1,
			wantCleanedUp: 1,
			wantH2Called:  true,
		},
		{
			name:          "aborts after first handler error, second handler not called",
			events:        []StoredEvent{newStoredEvent(1)},
			handlerErr:    errors.New("h1 error"),
			secondHandler: true,
			wantErr:       true,
			wantHandled:   1,
			wantCleanedUp: 0,
			wantH2Called:  false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &mockCleanUpStore{
				events:     tc.events,
				fetchErr:   tc.fetchErr,
				cleanUpErr: tc.cleanUpErr,
			}
			h := &mockHandler{err: tc.handlerErr}
			relay := NewTransientRelay("r", store, tc.opts...)
			relay.RegisterHandler(h)

			var h2 *mockHandler
			if tc.secondHandler {
				h2 = &mockHandler{}
				relay.RegisterHandler(h2)
			}

			err := relay.Run(context.Background())

			if (err != nil) != tc.wantErr {
				t.Fatalf("wantErr=%v, got %v", tc.wantErr, err)
			}
			if len(h.handleEvents) != tc.wantHandled {
				t.Errorf("expected %d handled events, got %d", tc.wantHandled, len(h.handleEvents))
			}
			if len(store.cleanedUp) != tc.wantCleanedUp {
				t.Errorf("expected %d cleaned-up events, got %d", tc.wantCleanedUp, len(store.cleanedUp))
			}
			if tc.secondHandler && h2.handleCalled != tc.wantH2Called {
				t.Errorf("h2 called=%v, want %v", h2.handleCalled, tc.wantH2Called)
			}
		})
	}
}

func TestTransientRelay_CleansUpEventsAsBatch(t *testing.T) {
	tests := []struct {
		name            string
		events          []StoredEvent
		handlerErr      error
		failOnNthHandle int // 1-based; 0 means never fail
		wantErr         bool
		wantCleanUpLens []int // expected lengths of each CleanUpEvents call
	}{
		{
			name:            "all events cleaned up in a single call",
			events:          []StoredEvent{newStoredEvent(1), newStoredEvent(2), newStoredEvent(3)},
			wantCleanUpLens: []int{3},
		},
		{
			name:            "empty batch does not invoke clean up",
			events:          nil,
			wantCleanUpLens: nil,
		},
		{
			name:            "partial batch cleaned up when handler fails mid-way",
			events:          []StoredEvent{newStoredEvent(1), newStoredEvent(2), newStoredEvent(3)},
			handlerErr:      errors.New("boom"),
			failOnNthHandle: 3,
			wantErr:         true,
			wantCleanUpLens: []int{2},
		},
		{
			name:            "no clean up call when first event already fails",
			events:          []StoredEvent{newStoredEvent(1), newStoredEvent(2)},
			handlerErr:      errors.New("boom"),
			failOnNthHandle: 1,
			wantErr:         true,
			wantCleanUpLens: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &recordingCleanUpStore{mockCleanUpStore: mockCleanUpStore{events: tc.events}}
			h := &countingHandler{err: tc.handlerErr, failOnCall: tc.failOnNthHandle}
			relay := NewTransientRelay("r", store)
			relay.RegisterHandler(h)

			err := relay.Run(context.Background())
			if (err != nil) != tc.wantErr {
				t.Fatalf("wantErr=%v, got %v", tc.wantErr, err)
			}
			if len(store.calls) != len(tc.wantCleanUpLens) {
				t.Fatalf("expected %d CleanUpEvents call(s), got %d", len(tc.wantCleanUpLens), len(store.calls))
			}
			for i, want := range tc.wantCleanUpLens {
				if len(store.calls[i]) != want {
					t.Errorf("call %d: expected %d events, got %d", i, want, len(store.calls[i]))
				}
			}
		})
	}
}

// countingHandler returns err on every Handle call when failOnCall == 0.
// When failOnCall > 0, it returns err only on that Nth Handle call (1-based)
// and returns nil on all other calls.
type countingHandler struct {
	calls      int
	err        error
	failOnCall int
}

func (c *countingHandler) Name() string { return "counting-handler" }

func (c *countingHandler) Handle(_ context.Context, _ StoredEvent) error {
	c.calls++
	if c.failOnCall == 0 {
		return c.err
	}
	if c.calls == c.failOnCall {
		return c.err
	}
	return nil
}

// recordingCleanUpStore records every CleanUpEvents invocation so tests can
// assert both the number of batch calls and the number of events per call.
type recordingCleanUpStore struct {
	mockCleanUpStore
	calls [][]StoredEvent
}

func (r *recordingCleanUpStore) CleanUpEvents(ctx context.Context, events []StoredEvent) error {
	r.calls = append(r.calls, append([]StoredEvent(nil), events...))
	return r.mockCleanUpStore.CleanUpEvents(ctx, events)
}

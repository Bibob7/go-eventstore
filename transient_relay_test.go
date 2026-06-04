package eventstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
)

type mockTransientStore struct {
	events     []StoredEvent
	fetchErr   error
	cleanUpErr error
	cleanedUp  []StoredEvent
}

func (m *mockTransientStore) FetchBatchOfEvents(_ context.Context, limit int) ([]StoredEvent, error) {
	if m.fetchErr != nil {
		return nil, m.fetchErr
	}
	if len(m.events) > limit {
		return m.events[:limit], nil
	}
	return m.events, nil
}

func (m *mockTransientStore) CleanUpEvents(_ context.Context, events []StoredEvent) error {
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
			relay := must(NewTransientHandlerRelay(tc.relayName, &mockTransientStore{}, func(WorkerContext) Handler { return &mockHandler{} }))
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
		opts          []RelayOption
		wantErr       bool
		wantHandled   int
		wantCleanedUp int
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
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &mockTransientStore{
				events:     tc.events,
				fetchErr:   tc.fetchErr,
				cleanUpErr: tc.cleanUpErr,
			}
			h := &mockHandler{err: tc.handlerErr}
			relay := must(NewTransientHandlerRelay("r", store, func(WorkerContext) Handler { return h }, tc.opts...))

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
			store := &recordingTransientStore{mockTransientStore: mockTransientStore{events: tc.events}}
			h := &countingHandler{err: tc.handlerErr, failOnCall: tc.failOnNthHandle}
			relay := must(NewTransientHandlerRelay("r", store, func(WorkerContext) Handler { return h }))

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

// recordingTransientStore records every CleanUpEvents invocation so tests can
// assert both the number of batch calls and the number of events per call.
type recordingTransientStore struct {
	mockTransientStore
	calls [][]StoredEvent
}

func (r *recordingTransientStore) CleanUpEvents(ctx context.Context, events []StoredEvent) error {
	r.calls = append(r.calls, append([]StoredEvent(nil), events...))
	return r.mockTransientStore.CleanUpEvents(ctx, events)
}

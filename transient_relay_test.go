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

func (m *mockCleanUpStore) Append(_ context.Context, _ ...DomainEvent) error { return nil }

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

func TestTransientRelay_CleansUpEachEventIndividually(t *testing.T) {
	events := []StoredEvent{newStoredEvent(1), newStoredEvent(2)}
	store := &mockCleanUpStore{events: events}
	relay := NewTransientRelay("r", store)
	relay.RegisterHandler(&mockHandler{})

	if err := relay.Run(context.Background()); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(store.cleanedUp) != 2 {
		t.Fatalf("expected 2 individual clean ups, got %d", len(store.cleanedUp))
	}
	if store.cleanedUp[0].ID != events[0].ID || store.cleanedUp[1].ID != events[1].ID {
		t.Error("clean-up order does not match event order")
	}
}
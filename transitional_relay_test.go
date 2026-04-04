package eventstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
)

type mockCleanUpStore struct {
	events       []StoredEvent
	fetchErr     error
	cleanUpErr   error
	cleanedUp    []StoredEvent
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

func TestTransitionalRelay_Name(t *testing.T) {
	relay := NewTransitionalRelay("my-relay", &mockCleanUpStore{})
	if relay.Name() != "my-relay" {
		t.Fatalf("expected name %q, got %q", "my-relay", relay.Name())
	}
}

func TestTransitionalRelay_NoEvents(t *testing.T) {
	store := &mockCleanUpStore{}
	relay := NewTransitionalRelay("r", store)

	if err := relay.Run(context.Background()); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(store.cleanedUp) != 0 {
		t.Errorf("expected no clean ups, got %d", len(store.cleanedUp))
	}
}

func TestTransitionalRelay_HandlesAndCleansUpAllEvents(t *testing.T) {
	store := &mockCleanUpStore{
		events: []StoredEvent{newStoredEvent(1), newStoredEvent(2), newStoredEvent(3)},
	}
	h := &mockHandler{}
	relay := NewTransitionalRelay("r", store)
	relay.RegisterHandler(h)

	if err := relay.Run(context.Background()); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(h.handleEvents) != 3 {
		t.Errorf("expected 3 handled events, got %d", len(h.handleEvents))
	}
	if len(store.cleanedUp) != 3 {
		t.Errorf("expected 3 cleaned-up events, got %d", len(store.cleanedUp))
	}
}

func TestTransitionalRelay_CleansUpEachEventIndividually(t *testing.T) {
	events := []StoredEvent{newStoredEvent(1), newStoredEvent(2)}
	store := &mockCleanUpStore{events: events}
	h := &mockHandler{}
	relay := NewTransitionalRelay("r", store)
	relay.RegisterHandler(h)

	if err := relay.Run(context.Background()); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(store.cleanedUp) != 2 {
		t.Fatalf("expected 2 individual clean ups, got %d", len(store.cleanedUp))
	}
	if store.cleanedUp[0].ID != events[0].ID {
		t.Errorf("first cleaned-up event mismatch")
	}
	if store.cleanedUp[1].ID != events[1].ID {
		t.Errorf("second cleaned-up event mismatch")
	}
}

func TestTransitionalRelay_HandlerError_NoCleanUp(t *testing.T) {
	store := &mockCleanUpStore{
		events: []StoredEvent{newStoredEvent(1)},
	}
	h := &mockHandler{err: errors.New("handler error")}
	relay := NewTransitionalRelay("r", store)
	relay.RegisterHandler(h)

	err := relay.Run(context.Background())

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if len(store.cleanedUp) != 0 {
		t.Errorf("expected no clean up after handler error, got %d", len(store.cleanedUp))
	}
}

func TestTransitionalRelay_ErrEventNotReadyToProcess_NoCleanUp(t *testing.T) {
	store := &mockCleanUpStore{
		events: []StoredEvent{newStoredEvent(1)},
	}
	h := &mockHandler{err: ErrEventNotReadyToProcess}
	relay := NewTransitionalRelay("r", store)
	relay.RegisterHandler(h)

	err := relay.Run(context.Background())

	if !errors.Is(err, ErrEventNotReadyToProcess) {
		t.Fatalf("expected ErrEventNotReadyToProcess, got %v", err)
	}
	if len(store.cleanedUp) != 0 {
		t.Errorf("expected no clean up, got %d", len(store.cleanedUp))
	}
}

func TestTransitionalRelay_FetchError(t *testing.T) {
	store := &mockCleanUpStore{fetchErr: errors.New("fetch error")}
	relay := NewTransitionalRelay("r", store)

	if err := relay.Run(context.Background()); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestTransitionalRelay_CleanUpError(t *testing.T) {
	store := &mockCleanUpStore{
		events:     []StoredEvent{newStoredEvent(1)},
		cleanUpErr: errors.New("cleanup error"),
	}
	h := &mockHandler{}
	relay := NewTransitionalRelay("r", store)
	relay.RegisterHandler(h)

	err := relay.Run(context.Background())

	if err == nil {
		t.Fatal("expected error from CleanUpEvents, got nil")
	}
}

func TestTransitionalRelay_BatchSize(t *testing.T) {
	store := &mockCleanUpStore{
		events: []StoredEvent{newStoredEvent(1), newStoredEvent(2), newStoredEvent(3)},
	}
	h := &mockHandler{}
	relay := NewTransitionalRelay("r", store, WithBatchSize(2))
	relay.RegisterHandler(h)

	if err := relay.Run(context.Background()); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(h.handleEvents) != 2 {
		t.Errorf("expected 2 events due to batch size, got %d", len(h.handleEvents))
	}
	if len(store.cleanedUp) != 2 {
		t.Errorf("expected 2 clean ups, got %d", len(store.cleanedUp))
	}
}

func TestTransitionalRelay_MultipleHandlers_AllCalled(t *testing.T) {
	store := &mockCleanUpStore{events: []StoredEvent{newStoredEvent(1)}}
	h1 := &mockHandler{}
	h2 := &mockHandler{}
	relay := NewTransitionalRelay("r", store)
	relay.RegisterHandler(h1, h2)

	if err := relay.Run(context.Background()); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(h1.handleEvents) != 1 {
		t.Errorf("expected h1 to handle 1 event, got %d", len(h1.handleEvents))
	}
	if len(h2.handleEvents) != 1 {
		t.Errorf("expected h2 to handle 1 event, got %d", len(h2.handleEvents))
	}
}

func TestTransitionalRelay_MultipleHandlers_AbortsOnFirstError(t *testing.T) {
	store := &mockCleanUpStore{events: []StoredEvent{newStoredEvent(1)}}
	h1 := &mockHandler{err: errors.New("h1 error")}
	h2 := &mockHandler{}
	relay := NewTransitionalRelay("r", store)
	relay.RegisterHandler(h1, h2)

	if err := relay.Run(context.Background()); err == nil {
		t.Fatal("expected error, got nil")
	}
	if h2.handleCalled {
		t.Error("expected h2 not to be called after h1 error")
	}
	if len(store.cleanedUp) != 0 {
		t.Errorf("expected no clean up after handler error, got %d", len(store.cleanedUp))
	}
}
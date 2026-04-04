package eventstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
)

// Mocks for the tests
type mockPointerStore struct {
	events []StoredEvent
	err    error
}

func (m *mockPointerStore) FetchBatchOfEventsSince(ctx context.Context, incrementID int64, limit int) ([]StoredEvent, error) {
	if m.err != nil {
		return nil, m.err
	}
	result := []StoredEvent{}
	for _, event := range m.events {
		if event.IncrementID > incrementID {
			result = append(result, event)
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

// Mock-Implementation for the Store interface to support Append method
func (m *mockPointerStore) Append(ctx context.Context, events ...DomainEvent) error {
	return nil
}

type mockIncrementIDStore struct {
	incrementIDs map[string]int64
	getErr       error
	setErr       error
}

func newMockIncrementIDStore() *mockIncrementIDStore {
	return &mockIncrementIDStore{
		incrementIDs: make(map[string]int64),
	}
}

func (m *mockIncrementIDStore) GetIncrementID(ctx context.Context, consumerName string) (int64, error) {
	if m.getErr != nil {
		return 0, m.getErr
	}
	return m.incrementIDs[consumerName], nil
}

func (m *mockIncrementIDStore) SetIncrementID(ctx context.Context, consumerName string, incrementID int64) error {
	if m.setErr != nil {
		return m.setErr
	}
	m.incrementIDs[consumerName] = incrementID
	return nil
}

type mockHandler struct {
	handleCalled bool
	handleEvents []StoredEvent
	err          error
}

func (m *mockHandler) Name() string {
	return "mock-handler"
}

func (m *mockHandler) Handle(ctx context.Context, event StoredEvent) error {
	m.handleCalled = true
	m.handleEvents = append(m.handleEvents, event)
	return m.err
}

func TestPointerRelay_Name(t *testing.T) {
	relay := NewPointerRelay("test-processor", nil, nil, WithBatchSize(10))

	if relay.Name() != "test-processor" {
		t.Errorf("Expected name 'test-processor', got '%s'", relay.Name())
	}
}

func TestPointerRelay_ProcessEvents_NoEvents(t *testing.T) {
	store := &mockPointerStore{
		events: []StoredEvent{},
	}
	incrementIDStore := newMockIncrementIDStore()
	relay := NewPointerRelay("test-processor", store, incrementIDStore, WithBatchSize(10))

	err := relay.Run(context.Background())

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestPointerRelay_ProcessEvents_Success(t *testing.T) {
	id1, _ := uuid.NewV4()
	id2, _ := uuid.NewV4()
	id3, _ := uuid.NewV4()
	events := []StoredEvent{
		{ID: id1, EntityID: id1, IncrementID: 1, EventType: "test-event", OccurredAt: time.Now()},
		{ID: id2, EntityID: id2, IncrementID: 2, EventType: "test-event", OccurredAt: time.Now()},
		{ID: id3, EntityID: id3, IncrementID: 3, EventType: "test-event", OccurredAt: time.Now()},
	}
	store := &mockPointerStore{
		events: events,
	}
	incrementIDStore := newMockIncrementIDStore()
	handler := &mockHandler{}
	relay := NewPointerRelay("test-processor", store, incrementIDStore, WithBatchSize(10))
	relay.RegisterHandler(handler)

	err := relay.Run(context.Background())

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !handler.handleCalled {
		t.Error("Handler should have been called")
	}
	if len(handler.handleEvents) != 3 {
		t.Errorf("Expected 3 events to be handled, got %d", len(handler.handleEvents))
	}
	lastID, err := incrementIDStore.GetIncrementID(context.Background(), "test-processor")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if lastID != 3 {
		t.Errorf("Expected last increment ID to be 3, got %d", lastID)
	}
}

func TestPointerRelay_ProcessEvents_GetIncrementIDError(t *testing.T) {
	store := &mockPointerStore{}
	incrementIDStore := newMockIncrementIDStore()
	incrementIDStore.getErr = errors.New("get error")
	relay := NewPointerRelay("test-processor", store, incrementIDStore, WithBatchSize(10))

	err := relay.Run(context.Background())

	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestPointerRelay_ProcessEvents_FetchEventsError(t *testing.T) {
	store := &mockPointerStore{
		err: errors.New("fetch error"),
	}
	incrementIDStore := newMockIncrementIDStore()
	relay := NewPointerRelay("test-processor", store, incrementIDStore, WithBatchSize(10))

	err := relay.Run(context.Background())

	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestPointerRelay_ProcessEvents_HandlerError(t *testing.T) {
	id1, _ := uuid.NewV4()
	events := []StoredEvent{
		{ID: id1, EntityID: id1, IncrementID: 1, EventType: "test-event", OccurredAt: time.Now()},
	}
	store := &mockPointerStore{
		events: events,
	}
	incrementIDStore := newMockIncrementIDStore()
	handler := &mockHandler{
		err: errors.New("handler error"),
	}
	relay := NewPointerRelay("test-processor", store, incrementIDStore, WithBatchSize(10))
	relay.RegisterHandler(handler)

	err := relay.Run(context.Background())

	if err == nil {
		t.Error("Expected error, got nil")
	}
	lastID, _ := incrementIDStore.GetIncrementID(context.Background(), "test-processor")
	if lastID != 0 {
		t.Errorf("Expected last increment ID to still be 0, got %d", lastID)
	}
}

func TestPointerRelay_ProcessEvents_BatchSize(t *testing.T) {
	id1, _ := uuid.NewV4()
	id2, _ := uuid.NewV4()
	id3, _ := uuid.NewV4()
	id4, _ := uuid.NewV4()
	id5, _ := uuid.NewV4()
	events := []StoredEvent{
		{ID: id1, EntityID: id1, IncrementID: 1, EventType: "test-event", OccurredAt: time.Now()},
		{ID: id2, EntityID: id2, IncrementID: 2, EventType: "test-event", OccurredAt: time.Now()},
		{ID: id3, EntityID: id3, IncrementID: 3, EventType: "test-event", OccurredAt: time.Now()},
		{ID: id4, EntityID: id4, IncrementID: 4, EventType: "test-event", OccurredAt: time.Now()},
		{ID: id5, EntityID: id5, IncrementID: 5, EventType: "test-event", OccurredAt: time.Now()},
	}
	store := &mockPointerStore{
		events: events,
	}
	incrementIDStore := newMockIncrementIDStore()
	handler := &mockHandler{}
	relay := NewPointerRelay("test-processor", store, incrementIDStore, WithBatchSize(2))
	relay.RegisterHandler(handler)

	err := relay.Run(context.Background())

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if len(handler.handleEvents) != 2 {
		t.Errorf("Expected 2 events to be handled due to batch size, got %d", len(handler.handleEvents))
	}
	lastID, _ := incrementIDStore.GetIncrementID(context.Background(), "test-processor")
	if lastID != 2 {
		t.Errorf("Expected last increment ID to be 2, got %d", lastID)
	}
}

// ---- Additional tests for delayed relay behaviors ----

// delayedRelayStub implements Relay and returns a preconfigured error from Run.
type delayedRelayStub struct {
	name       string
	processErr error
}

func (s *delayedRelayStub) Name() string                             { return s.name }
func (s *delayedRelayStub) RegisterHandler(handler ...Handler) Relay { return s }
func (s *delayedRelayStub) Run(ctx context.Context) error            { return s.processErr }

func TestDelayedRelay_PropagatesOtherErrors(t *testing.T) {
	expected := errors.New("other")
	dp := newDelayedRelay(&delayedRelayStub{name: "dp", processErr: expected}, 50*time.Millisecond)
	start := time.Now()
	err := dp.Run(context.Background())
	elapsed := time.Since(start)
	if !errors.Is(err, expected) {
		t.Fatalf("expected error to propagate, got %v", err)
	}
	if elapsed >= 50*time.Millisecond {
		t.Fatalf("should not delay on other errors; elapsed=%v", elapsed)
	}
}

func TestDelayedRelay_DelayOnNotReady(t *testing.T) {
	delay := 40 * time.Millisecond
	dp := newDelayedRelay(&delayedRelayStub{name: "dp", processErr: ErrEventNotReadyToProcess}, delay)
	start := time.Now()
	err := dp.Run(context.Background())
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if elapsed < delay {
		t.Fatalf("expected delay of at least %v, got %v", delay, elapsed)
	}
}

func TestDelayedRelay_ContextCancelDuringWait(t *testing.T) {
	delay := 200 * time.Millisecond
	dp := newDelayedRelay(&delayedRelayStub{name: "dp", processErr: ErrEventNotReadyToProcess}, delay)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err := dp.Run(ctx)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("expected context error, got nil")
	}
	if elapsed > delay {
		t.Fatalf("expected to return before full delay due to cancel; elapsed=%v delay=%v", elapsed, delay)
	}
}

func TestPointerRelay_WithHandleDelay_AppliedBetweenEvents(t *testing.T) {
	id1, _ := uuid.NewV4()
	id2, _ := uuid.NewV4()
	id3, _ := uuid.NewV4()
	events := []StoredEvent{
		{ID: id1, EntityID: id1, IncrementID: 1, EventType: "test-event", OccurredAt: time.Now()},
		{ID: id2, EntityID: id2, IncrementID: 2, EventType: "test-event", OccurredAt: time.Now()},
		{ID: id3, EntityID: id3, IncrementID: 3, EventType: "test-event", OccurredAt: time.Now()},
	}
	store := &mockPointerStore{events: events}
	inc := newMockIncrementIDStore()
	h := &mockHandler{}
	delay := 20 * time.Millisecond
	p := NewPointerRelay("test-processor", store, inc, WithHandleDelay(delay))
	p.RegisterHandler(h)

	start := time.Now()
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	elapsed := time.Since(start)

	// 3 events -> 3 delays; total must be at least 3x delay
	if elapsed < 3*delay {
		t.Errorf("expected at least %v elapsed for 3 events, got %v", 3*delay, elapsed)
	}
	if len(h.handleEvents) != 3 {
		t.Errorf("expected all 3 events handled, got %d", len(h.handleEvents))
	}
}

func TestPointerRelay_WithHandleDelay_ContextCancelledDuringDelay(t *testing.T) {
	id1, _ := uuid.NewV4()
	id2, _ := uuid.NewV4()
	events := []StoredEvent{
		{ID: id1, EntityID: id1, IncrementID: 1, EventType: "test-event", OccurredAt: time.Now()},
		{ID: id2, EntityID: id2, IncrementID: 2, EventType: "test-event", OccurredAt: time.Now()},
	}
	store := &mockPointerStore{events: events}
	inc := newMockIncrementIDStore()
	h := &mockHandler{}
	delay := 200 * time.Millisecond
	p := NewPointerRelay("test-processor", store, inc, WithHandleDelay(delay))
	p.RegisterHandler(h)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := p.Run(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected context error, got nil")
	}
	// Should have returned early -- well before the full delay
	if elapsed >= delay {
		t.Errorf("expected early return due to context cancel, elapsed=%v", elapsed)
	}
	// Only the first event is processed; second is cancelled during the delay after the first
	if len(h.handleEvents) != 1 {
		t.Errorf("expected 1 event handled before cancel, got %d", len(h.handleEvents))
	}
}

func TestPointerRelay_WithHandleDelay_ZeroDelay_NoWait(t *testing.T) {
	id1, _ := uuid.NewV4()
	id2, _ := uuid.NewV4()
	events := []StoredEvent{
		{ID: id1, EntityID: id1, IncrementID: 1, EventType: "test-event", OccurredAt: time.Now()},
		{ID: id2, EntityID: id2, IncrementID: 2, EventType: "test-event", OccurredAt: time.Now()},
	}
	store := &mockPointerStore{events: events}
	inc := newMockIncrementIDStore()
	h := &mockHandler{}
	// No WithHandleDelay option -> delay is zero
	p := NewPointerRelay("test-processor", store, inc)
	p.RegisterHandler(h)

	start := time.Now()
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	elapsed := time.Since(start)

	if elapsed >= 20*time.Millisecond {
		t.Errorf("expected near-instant processing without delay, got %v", elapsed)
	}
	if len(h.handleEvents) != 2 {
		t.Errorf("expected 2 events handled, got %d", len(h.handleEvents))
	}
}

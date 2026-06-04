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

type mockIncrementIDStore struct {
	incrementIDs map[string]int64
	getErr       error
	setErr       error
	setHook      func(consumerName string)
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

func (m *mockIncrementIDStore) SetIncrementID(ctx context.Context, consumerName string, expectedPreviousID int64, incrementID int64) error {
	if m.setErr != nil {
		return m.setErr
	}
	if m.setHook != nil {
		m.setHook(consumerName)
	}
	if m.incrementIDs[consumerName] != expectedPreviousID {
		return ErrIncrementIDConflict
	}
	m.incrementIDs[consumerName] = incrementID
	return nil
}

type mockHandler struct {
	handleCalled bool
	handleEvents []StoredEvent
	err          error
	// failOnCall, when > 0, causes Handle to return err only on the Nth call
	// (1-based). When 0, err is returned on every call.
	failOnCall int
	calls      int
}

func (m *mockHandler) Name() string {
	return "mock-handler"
}

func (m *mockHandler) Handle(ctx context.Context, event StoredEvent) error {
	m.handleCalled = true
	m.handleEvents = append(m.handleEvents, event)
	m.calls++
	if m.failOnCall == 0 {
		return m.err
	}
	if m.calls == m.failOnCall {
		return m.err
	}
	return nil
}

func newEvents(incrementIDs ...int64) []StoredEvent {
	events := make([]StoredEvent, len(incrementIDs))
	for i, id := range incrementIDs {
		uid, _ := uuid.NewV4()
		events[i] = StoredEvent{ID: uid, EntityID: uid, IncrementID: id, EventType: "test-event", OccurredAt: time.Now()}
	}
	return events
}

func TestPointerRelay_Name(t *testing.T) {
	tests := []struct {
		name      string
		relayName string
	}{
		{name: "returns configured name", relayName: "test-processor"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			relay := NewPointerHandlerRelay(tc.relayName, nil, nil, func(WorkerContext) Handler { return &mockHandler{} })
			if relay.Name() != tc.relayName {
				t.Errorf("expected name %q, got %q", tc.relayName, relay.Name())
			}
		})
	}
}

func TestRelay_RunErrorsOnNilFactory(t *testing.T) {
	tests := []struct {
		name  string
		relay Relay
	}{
		{
			name:  "NewPointerHandlerRelay",
			relay: NewPointerHandlerRelay("r", &mockPointerStore{events: newEvents(1)}, newMockIncrementIDStore(), nil),
		},
		{
			name:  "NewPointerBatchHandlerRelay",
			relay: NewPointerBatchHandlerRelay("r", &mockPointerStore{events: newEvents(1)}, newMockIncrementIDStore(), nil),
		},
		{
			name:  "NewTransientHandlerRelay",
			relay: NewTransientHandlerRelay("r", &mockTransientStore{events: newEvents(1)}, nil),
		},
		{
			name:  "NewTransientBatchHandlerRelay",
			relay: NewTransientBatchHandlerRelay("r", &mockTransientStore{events: newEvents(1)}, nil),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.relay.Run(context.Background()); !errors.Is(err, ErrNilFactory) {
				t.Errorf("expected ErrNilFactory from Run, got %v", err)
			}
		})
	}
}

func TestPointerRelay_Run(t *testing.T) {
	tests := []struct {
		name            string
		events          []StoredEvent
		fetchErr        error
		getErr          error
		setErr          error
		handlerErr      error
		failOnNthHandle int
		opts            []RelayOption
		wantErr         bool
		wantHandled     int
		wantLastID      int64
	}{
		{
			name:        "no events",
			wantHandled: 0,
			wantLastID:  0,
		},
		{
			name:        "processes all events and saves progress",
			events:      newEvents(1, 2, 3),
			wantHandled: 3,
			wantLastID:  3,
		},
		{
			name:        "respects batch size",
			events:      newEvents(1, 2, 3, 4, 5),
			opts:        []RelayOption{WithBatchSize(2)},
			wantHandled: 2,
			wantLastID:  2,
		},
		{
			name:    "GetIncrementID error propagates",
			getErr:  errors.New("get error"),
			wantErr: true,
		},
		{
			name:     "FetchBatchOfEventsSince error propagates",
			fetchErr: errors.New("fetch error"),
			wantErr:  true,
		},
		{
			name:        "handler error aborts without saving progress",
			events:      newEvents(1),
			handlerErr:  errors.New("handler error"),
			wantErr:     true,
			wantHandled: 1,
			wantLastID:  0,
		},
		{
			name:        "ErrEventNotReadyToProcess aborts without saving progress",
			events:      newEvents(1),
			handlerErr:  ErrEventNotReadyToProcess,
			wantErr:     true,
			wantHandled: 1,
			wantLastID:  0,
		},
		{
			name:        "SetIncrementID error propagates",
			events:      newEvents(1),
			setErr:      errors.New("set error"),
			wantErr:     true,
			wantHandled: 1,
		},
		{
			name:            "partial progress saved when handler fails mid-batch",
			events:          newEvents(1, 2, 3),
			handlerErr:      errors.New("boom"),
			failOnNthHandle: 3,
			wantErr:         true,
			wantHandled:     3,
			wantLastID:      2,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &mockPointerStore{events: tc.events, err: tc.fetchErr}
			inc := newMockIncrementIDStore()
			inc.getErr = tc.getErr
			inc.setErr = tc.setErr
			h := &mockHandler{err: tc.handlerErr, failOnCall: tc.failOnNthHandle}
			relay := NewPointerHandlerRelay("test-processor", store, inc, func(WorkerContext) Handler { return h }, tc.opts...)

			err := relay.Run(context.Background())

			if (err != nil) != tc.wantErr {
				t.Fatalf("wantErr=%v, got %v", tc.wantErr, err)
			}
			if len(h.handleEvents) != tc.wantHandled {
				t.Errorf("expected %d handled events, got %d", tc.wantHandled, len(h.handleEvents))
			}
			lastID, _ := inc.GetIncrementID(context.Background(), "test-processor")
			if lastID != tc.wantLastID {
				t.Errorf("expected last increment ID %d, got %d", tc.wantLastID, lastID)
			}
		})
	}
}

func TestPointerRelay_IncrementIDConflictPropagates(t *testing.T) {
	store := &mockPointerStore{events: newEvents(1)}
	inc := newMockIncrementIDStore()
	inc.setHook = func(consumerName string) {
		inc.incrementIDs[consumerName] = 99
	}
	h := &mockHandler{}
	relay := NewPointerHandlerRelay("test-processor", store, inc, func(WorkerContext) Handler { return h })

	err := relay.Run(context.Background())
	if err == nil || !errors.Is(err, ErrIncrementIDConflict) {
		t.Fatalf("expected ErrIncrementIDConflict, got %v", err)
	}
}

func TestPointerRelay_BatchDelayOptions(t *testing.T) {
	delay := 40 * time.Millisecond

	tests := []struct {
		name       string
		opts       []RelayOption
		handlerErr error
		minElapsed time.Duration
	}{
		{
			name:       "WithBatchDelay delays after every run",
			opts:       []RelayOption{WithBatchDelay(delay)},
			minElapsed: delay,
		},
		{
			name:       "WithConditionalBatchDelay delays on ErrEventNotReadyToProcess",
			opts:       []RelayOption{WithConditionalBatchDelay(delay)},
			handlerErr: ErrEventNotReadyToProcess,
			minElapsed: delay,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &mockPointerStore{events: newEvents(1)}
			inc := newMockIncrementIDStore()
			h := &mockHandler{err: tc.handlerErr}
			relay := NewPointerHandlerRelay("test-processor", store, inc, func(WorkerContext) Handler { return h }, tc.opts...)

			start := time.Now()
			_ = relay.Run(context.Background())
			elapsed := time.Since(start)

			if elapsed < tc.minElapsed {
				t.Errorf("expected at least %v elapsed, got %v", tc.minElapsed, elapsed)
			}
		})
	}
}

// ---- Stub for decorator tests ----

// relayStub implements Relay and returns a preconfigured error from Run.
// Used to test the relay decorators (delayedRelay, batchDelayedRelay)
// without pulling in a real store.
type relayStub struct {
	name       string
	processErr error
}

func (s *relayStub) Name() string                  { return s.name }
func (s *relayStub) Run(ctx context.Context) error { return s.processErr }

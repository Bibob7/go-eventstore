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
			relay := NewPointerRelay(tc.relayName, nil, nil)
			if relay.Name() != tc.relayName {
				t.Errorf("expected name %q, got %q", tc.relayName, relay.Name())
			}
		})
	}
}

func TestPointerRelay_Run(t *testing.T) {
	tests := []struct {
		name        string
		events      []StoredEvent
		fetchErr    error
		getErr      error
		setErr      error
		handlerErr  error
		opts        []RelayOption
		wantErr     bool
		wantHandled int
		wantLastID  int64
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
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &mockPointerStore{events: tc.events, err: tc.fetchErr}
			inc := newMockIncrementIDStore()
			inc.getErr = tc.getErr
			inc.setErr = tc.setErr
			h := &mockHandler{err: tc.handlerErr}
			relay := NewPointerRelay("test-processor", store, inc, tc.opts...)
			relay.RegisterHandler(h)

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

func TestPointerRelay_MultipleHandlers(t *testing.T) {
	tests := []struct {
		name         string
		h1Err        error
		wantH2Called bool
		wantLastID   int64
		wantErr      bool
	}{
		{
			name:         "all handlers called on success",
			wantH2Called: true,
			wantLastID:   1,
		},
		{
			name:         "aborts after first handler error",
			h1Err:        errors.New("h1 error"),
			wantH2Called: false,
			wantLastID:   0,
			wantErr:      true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &mockPointerStore{events: newEvents(1)}
			inc := newMockIncrementIDStore()
			h1 := &mockHandler{err: tc.h1Err}
			h2 := &mockHandler{}
			relay := NewPointerRelay("test-processor", store, inc)
			relay.RegisterHandler(h1, h2)

			err := relay.Run(context.Background())

			if (err != nil) != tc.wantErr {
				t.Fatalf("wantErr=%v, got %v", tc.wantErr, err)
			}
			if h2.handleCalled != tc.wantH2Called {
				t.Errorf("h2 called=%v, want %v", h2.handleCalled, tc.wantH2Called)
			}
			lastID, _ := inc.GetIncrementID(context.Background(), "test-processor")
			if lastID != tc.wantLastID {
				t.Errorf("expected last increment ID %d, got %d", tc.wantLastID, lastID)
			}
		})
	}
}

func TestPointerRelay_WithHandleDelay(t *testing.T) {
	delay := 20 * time.Millisecond
	cancelAfter := 30 * time.Millisecond
	longDelay := 200 * time.Millisecond

	tests := []struct {
		name        string
		events      []StoredEvent
		delay       time.Duration
		cancelAfter time.Duration
		wantErr     bool
		wantHandled int
		minElapsed  time.Duration
		maxElapsed  time.Duration
	}{
		{
			name:        "delay applied between events",
			events:      newEvents(1, 2, 3),
			delay:       delay,
			wantHandled: 3,
			minElapsed:  3 * delay,
		},
		{
			name:        "context cancel during delay stops processing",
			events:      newEvents(1, 2),
			delay:       longDelay,
			cancelAfter: cancelAfter,
			wantErr:     true,
			wantHandled: 1,
			maxElapsed:  longDelay,
		},
		{
			name:        "zero delay does not wait",
			events:      newEvents(1, 2),
			delay:       0,
			wantHandled: 2,
			maxElapsed:  20 * time.Millisecond,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &mockPointerStore{events: tc.events}
			inc := newMockIncrementIDStore()
			h := &mockHandler{}
			relay := NewPointerRelay("test-processor", store, inc, WithHandleDelay(tc.delay))
			relay.RegisterHandler(h)

			ctx := context.Background()
			if tc.cancelAfter > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				go func() {
					time.Sleep(tc.cancelAfter)
					cancel()
				}()
			}

			start := time.Now()
			err := relay.Run(ctx)
			elapsed := time.Since(start)

			if (err != nil) != tc.wantErr {
				t.Fatalf("wantErr=%v, got %v", tc.wantErr, err)
			}
			if len(h.handleEvents) != tc.wantHandled {
				t.Errorf("expected %d handled events, got %d", tc.wantHandled, len(h.handleEvents))
			}
			if tc.minElapsed > 0 && elapsed < tc.minElapsed {
				t.Errorf("expected at least %v elapsed, got %v", tc.minElapsed, elapsed)
			}
			if tc.maxElapsed > 0 && elapsed >= tc.maxElapsed {
				t.Errorf("expected less than %v elapsed, got %v", tc.maxElapsed, elapsed)
			}
		})
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
			relay := NewPointerRelay("test-processor", store, inc, tc.opts...)
			relay.RegisterHandler(h)

			start := time.Now()
			_ = relay.Run(context.Background())
			elapsed := time.Since(start)

			if elapsed < tc.minElapsed {
				t.Errorf("expected at least %v elapsed, got %v", tc.minElapsed, elapsed)
			}
		})
	}
}

// ---- Stubs for decorator tests ----

// delayedRelayStub implements Relay and returns a preconfigured error from Run.
type delayedRelayStub struct {
	name       string
	processErr error
}

func (s *delayedRelayStub) Name() string                             { return s.name }
func (s *delayedRelayStub) RegisterHandler(handler ...Handler) Relay { return s }
func (s *delayedRelayStub) Run(ctx context.Context) error            { return s.processErr }

package eventstore

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
)

// Benchmarks in this file measure the hot paths of the two relay flavours
// shipped by this package: the transient (work-queue) relay and the pointer
// (cursor) relay. They use in-memory stores so they can be run anywhere
// without external dependencies, while still modelling the per-batch
// allocation / locking patterns the real database-backed stores exhibit.
//
// Run with:
//   go test -bench=Benchmark -benchmem -run=^$ ./...
//   go test -bench=BenchmarkTransient -benchtime=2s -benchmem ./...
//   go test -bench=BenchmarkPointer -benchtime=2s -benchmem ./...

// ---- Shared in-memory test doubles ---------------------------------------

// benchDomainEvent is a minimal DomainEvent used to seed the relay pipelines.
type benchDomainEvent struct {
	id        uuid.UUID
	streamID  uuid.UUID
	eventType string
	occurred  time.Time
	metadata  Metadata
	version   int
}

func (e *benchDomainEvent) ID() uuid.UUID         { return e.id }
func (e *benchDomainEvent) StreamID() uuid.UUID   { return e.streamID }
func (e *benchDomainEvent) EventType() string     { return e.eventType }
func (e *benchDomainEvent) OccurredAt() time.Time { return e.occurred }
func (e *benchDomainEvent) Metadata() Metadata    { return e.metadata }
func (e *benchDomainEvent) StreamVersion() int    { return e.version }

func newBenchEvent(id int64) DomainEvent {
	uid, _ := uuid.NewV4()
	return &benchDomainEvent{
		id:        uid,
		streamID:  uid,
		eventType: "bench-event",
		occurred:  time.Now(),
	}
}

// benchTransientStore is an in-memory TransientStore that mimics the
// "fetch head, delete after handle" semantics of a queue-style event store.
type benchTransientStore struct {
	mu     sync.Mutex
	events []StoredEvent
	// nextID mirrors a database autoincrement; the relay never reads it but
	// keeping it lets us produce deterministic, ascending IncrementIDs.
	nextID int64
}

func (s *benchTransientStore) Append(_ context.Context, events ...DomainEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range events {
		be, ok := e.(*benchDomainEvent)
		if !ok {
			return fmt.Errorf("unexpected event type %T", e)
		}
		s.nextID++
		s.events = append(s.events, StoredEvent{
			IncrementID:   s.nextID,
			ID:            be.id,
			StreamID:      be.streamID,
			EventType:     be.eventType,
			Payload:       `{"bench":true}`,
			OccurredAt:    be.occurred,
			StreamVersion: be.version,
		})
	}
	return nil
}

func (s *benchTransientStore) FetchBatchOfEvents(_ context.Context, limit int) ([]StoredEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.events) == 0 {
		return nil, nil
	}
	if len(s.events) > limit {
		out := make([]StoredEvent, limit)
		copy(out, s.events[:limit])
		return out, nil
	}
	out := make([]StoredEvent, len(s.events))
	copy(out, s.events)
	return out, nil
}

func (s *benchTransientStore) CleanUpEvents(_ context.Context, events []StoredEvent) error {
	if len(events) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	remove := make(map[int64]struct{}, len(events))
	for _, e := range events {
		remove[e.IncrementID] = struct{}{}
	}
	kept := s.events[:0]
	for _, e := range s.events {
		if _, drop := remove[e.IncrementID]; !drop {
			kept = append(kept, e)
		}
	}
	// Rebuild slice so the underlying array can be GC'd.
	s.events = append([]StoredEvent(nil), kept...)
	return nil
}

// benchPointerStore is an in-memory PointerStore that returns events
// strictly after lastIncrementID, ordered by IncrementID ascending.
type benchPointerStore struct {
	mu     sync.RWMutex
	events []StoredEvent
}

func (s *benchPointerStore) Append(_ context.Context, events ...DomainEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range events {
		be, ok := e.(*benchDomainEvent)
		if !ok {
			return fmt.Errorf("unexpected event type %T", e)
		}
		s.events = append(s.events, StoredEvent{
			IncrementID:   int64(len(s.events)) + 1,
			ID:            be.id,
			StreamID:      be.streamID,
			EventType:     be.eventType,
			Payload:       `{"bench":true}`,
			OccurredAt:    be.occurred,
			Metadata:      be.metadata,
			StreamVersion: be.version,
		})
	}
	return nil
}

func (s *benchPointerStore) FetchBatchOfEventsSince(_ context.Context, lastIncrementID int64, limit int) ([]StoredEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]StoredEvent, 0, limit)
	for _, e := range s.events {
		if e.IncrementID > lastIncrementID {
			out = append(out, e)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

// benchIncrementIDStore is a tiny concurrent-safe IncrementIDStore.
// It mimics the contract that a write fails if the previously stored value
// has moved underneath us (ErrIncrementIDConflict).
type benchIncrementIDStore struct {
	mu   sync.Mutex
	vals map[string]int64
}

func newBenchIncrementIDStore() *benchIncrementIDStore {
	return &benchIncrementIDStore{vals: make(map[string]int64)}
}

func (s *benchIncrementIDStore) GetIncrementID(_ context.Context, name string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.vals[name], nil
}

func (s *benchIncrementIDStore) SetIncrementID(_ context.Context, name string, expected, next int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.vals[name] != expected {
		return ErrIncrementIDConflict
	}
	s.vals[name] = next
	return nil
}

// benchHandler is a no-op handler used to keep the focus on relay overhead.
type benchHandler struct {
	calls atomic.Int64
}

func (h *benchHandler) Name() string { return "bench-handler" }
func (h *benchHandler) Handle(_ context.Context, _ StoredEvent) error {
	h.calls.Add(1)
	return nil
}

// ---- Benchmarks: TransientEventStore relay -------------------------------

// BenchmarkTransientRelay measures the cost of a single Run call against an
// already-populated transient store, varying the in-flight batch size.
// It is the transient-relay analogue of BenchmarkPointerRelay_Throughput.
func BenchmarkTransientRelay(b *testing.B) {
	cases := []struct {
		name      string
		batchSize int
	}{
		{name: "BatchSize_10", batchSize: 10},
		{name: "BatchSize_100", batchSize: 100},
		{name: "BatchSize_500", batchSize: 500},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			store := &benchTransientStore{}
			// Pre-populate once so the benchmark measures relay + handler work
			// rather than the cost of running against an empty store.
			const seed = 5_000
			events := make([]DomainEvent, seed)
			for i := range events {
				events[i] = newBenchEvent(int64(i + 1))
			}
			if err := store.Append(context.Background(), events...); err != nil {
				b.Fatalf("seed: %v", err)
			}

			handler := &benchHandler{}
			relay := NewTransientHandlerRelay("bench-transient", store, func(WorkerContext) Handler { return handler }, WithBatchSize(tc.batchSize))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := relay.Run(context.Background()); err != nil {
					b.Fatalf("relay run: %v", err)
				}
				// If the relay drained the queue mid-run, refill so the next
				// iteration still measures work instead of early-exit.
				store.mu.Lock()
				if len(store.events) == 0 {
					for j := int64(1); j <= seed; j++ {
						store.nextID++
						uid, _ := uuid.NewV4()
						store.events = append(store.events, StoredEvent{
							IncrementID: store.nextID,
							ID:          uid,
							StreamID:    uid,
							EventType:   "bench-event",
							Payload:     `{"bench":true}`,
							OccurredAt:  time.Now(),
							// Version is intentionally 0: bench events are
							// single-shot and not concerned with per-stream
							// ordering, so a default of 0 is fine.
						})
					}
				}
				store.mu.Unlock()
			}
		})
	}
}

// BenchmarkTransientRelay_AppendAndDrain measures the end-to-end cost of
// writing events and then draining them through a transient relay. It
// captures the full transient use case ("work queue") including the
// per-batch CleanUpEvents round trip.
func BenchmarkTransientRelay_AppendAndDrain(b *testing.B) {
	cases := []struct {
		name      string
		batchSize int
	}{
		{name: "BatchSize_10", batchSize: 10},
		{name: "BatchSize_100", batchSize: 100},
		{name: "BatchSize_500", batchSize: 500},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			handler := &benchHandler{}
			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				store := &benchTransientStore{}
				relay := NewTransientHandlerRelay("bench-transient", store, func(WorkerContext) Handler { return handler }, WithBatchSize(tc.batchSize))

				// Append a fixed payload per iteration so we measure steady-state
				// append+relay cost rather than growing memory pressure.
				const perIter = 1_000
				events := make([]DomainEvent, perIter)
				for j := range events {
					events[j] = newBenchEvent(int64(j + 1))
				}
				if err := store.Append(ctx, events...); err != nil {
					b.Fatalf("append: %v", err)
				}

				for {
					if err := relay.Run(ctx); err != nil {
						b.Fatalf("relay run: %v", err)
					}
					store.mu.Lock()
					done := len(store.events) == 0
					store.mu.Unlock()
					if done {
						break
					}
				}
			}
		})
	}
}

// ---- Benchmarks: PointerBaseEventStore relay -----------------------------

// BenchmarkPointerRelay measures the cost of repeatedly running a pointer
// relay against a fully populated store with a saved cursor. Each run
// fetches a batch, hands events to the handler, and persists the new
// IncrementID — i.e. the full cursor-based use case.
func BenchmarkPointerRelay(b *testing.B) {
	cases := []struct {
		name      string
		batchSize int
	}{
		{name: "BatchSize_10", batchSize: 10},
		{name: "BatchSize_100", batchSize: 100},
		{name: "BatchSize_500", batchSize: 500},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			store := &benchPointerStore{}
			incStore := newBenchIncrementIDStore()
			const total = 20_000
			events := make([]DomainEvent, total)
			for i := range events {
				events[i] = newBenchEvent(int64(i + 1))
			}
			if err := store.Append(context.Background(), events...); err != nil {
				b.Fatalf("seed: %v", err)
			}

			handler := &benchHandler{}
			relay := NewPointerHandlerRelay("bench-pointer", store, incStore, func(WorkerContext) Handler { return handler }, WithBatchSize(tc.batchSize))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Reset the relay back to the start of the store so every
				// iteration performs a full sweep; this also exercises the
				// GetIncrementID -> FetchBatch -> SetIncrementID path.
				if err := incStore.SetIncrementID(context.Background(), "bench-pointer", incStore.vals["bench-pointer"], 0); err != nil {
					b.Fatalf("reset cursor: %v", err)
				}
				handler.calls.Store(0)
				if err := drainPointerRelay(relay, handler); err != nil {
					b.Fatalf("drain: %v", err)
				}
			}
		})
	}
}

// BenchmarkPointerRelay_AppendAndDrain measures the end-to-end pointer
// relay use case: write events, then run the relay until the saved
// cursor catches up. It is the pointer-relay analogue of
// BenchmarkTransientRelay_AppendAndDrain.
func BenchmarkPointerRelay_AppendAndDrain(b *testing.B) {
	cases := []struct {
		name      string
		batchSize int
	}{
		{name: "BatchSize_10", batchSize: 10},
		{name: "BatchSize_100", batchSize: 100},
		{name: "BatchSize_500", batchSize: 500},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			handler := &benchHandler{}
			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				store := &benchPointerStore{}
				incStore := newBenchIncrementIDStore()
				relay := NewPointerHandlerRelay("bench-pointer", store, incStore, func(WorkerContext) Handler { return handler }, WithBatchSize(tc.batchSize))

				const perIter = 1_000
				events := make([]DomainEvent, perIter)
				for j := range events {
					events[j] = newBenchEvent(int64(j + 1))
				}
				if err := store.Append(ctx, events...); err != nil {
					b.Fatalf("append: %v", err)
				}

				if err := drainPointerRelay(relay, handler); err != nil {
					b.Fatalf("drain: %v", err)
				}
			}
		})
	}
}

// BenchmarkPointerRelay_Idle measures the cost of polling an empty store —
// the typical "no work, just check again" case for a long-running relay.
func BenchmarkPointerRelay_Idle(b *testing.B) {
	store := &benchPointerStore{}
	incStore := newBenchIncrementIDStore()
	handler := &benchHandler{}
	relay := NewPointerHandlerRelay("bench-pointer-idle", store, incStore, func(WorkerContext) Handler { return handler })

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := relay.Run(context.Background()); err != nil {
			b.Fatalf("idle run: %v", err)
		}
		_ = handler
	}
}

// ---- Helpers -------------------------------------------------------------

// drainPointerRelay runs the relay once with a non-empty store, then once
// more so the second call observes an empty FetchBatchOfEventsSince result
// and returns nil. That mirrors the contract that a no-op Run returns nil
// when no events are available, and matches what the benchmark loop needs:
// one batch of work followed by a no-op confirmation.
func drainPointerRelay(relay Relay, _ *benchHandler) error {
	ctx := context.Background()
	if err := relay.Run(ctx); err != nil {
		return err
	}
	return relay.Run(ctx)
}

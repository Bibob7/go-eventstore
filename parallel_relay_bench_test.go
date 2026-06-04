package eventstore

import (
	"context"
	"testing"

	"github.com/gofrs/uuid/v5"
)

// BenchmarkParallelPointerRelay measures the throughput of a PointerRelay
// across varying worker counts and batch sizes. The fixture spreads events
// across 100 distinct EntityIDs (200 events per entity) so workers are
// evenly partitioned and stream consistency is exercised. Run with:
//
//	go test -bench=BenchmarkParallelPointerRelay -benchmem -run=^$ ./...
func BenchmarkParallelPointerRelay(b *testing.B) {
	cases := []struct {
		name        string
		parallelism int
		batchSize   int
	}{
		{"P1_Batch100", 1, 100},
		{"P2_Batch100", 2, 100},
		{"P4_Batch100", 4, 100},
		{"P8_Batch100", 8, 100},
		{"P1_Batch500", 1, 500},
		{"P4_Batch500", 4, 500},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			store := &benchPointerStore{}
			incStore := newBenchIncrementIDStore()

			// Seed 100 distinct entities with 200 events each, in a
			// round-robin pattern so pickWorker distributes them across
			// all available workers.
			entityIDs := make([]uuid.UUID, 100)
			for i := range entityIDs {
				entityIDs[i] = uuid.Must(uuid.NewV4())
			}
			const totalEntities = 100
			const eventsPerEntity = 200
			domainEvents := make([]DomainEvent, 0, totalEntities*eventsPerEntity)
			for i := 0; i < totalEntities*eventsPerEntity; i++ {
				ev := newBenchEvent(int64(i + 1)).(*benchDomainEvent)
				ev.entityID = entityIDs[i%totalEntities]
				domainEvents = append(domainEvents, ev)
			}
			if err := store.Append(context.Background(), domainEvents...); err != nil {
				b.Fatalf("seed: %v", err)
			}

			handler := &benchHandler{}
			relay := NewPointerHandlerRelay(
				"bench-parallel",
				store, incStore,
				func(WorkerContext) Handler { return handler },
				WithBatchSize(tc.batchSize),
				WithParallelism(tc.parallelism),
			)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := incStore.SetIncrementID(
					context.Background(),
					"bench-parallel",
					incStore.vals["bench-parallel"],
					0,
				); err != nil {
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

// benchWorkHandler simulates a handler that spends CPU time per event, so
// parallelism benchmarks reflect real handler cost instead of pure
// dispatch overhead. Each worker gets its own instance (via the factory),
// so the sink field is written by a single goroutine and stays race-free.
type benchWorkHandler struct {
	iterations int
	sink       uint64
}

func (h *benchWorkHandler) Name() string { return "bench-work-handler" }

func (h *benchWorkHandler) Handle(_ context.Context, _ StoredEvent) error {
	x := h.sink
	for i := 0; i < h.iterations; i++ {
		// Cheap, non-eliminable arithmetic (an LCG step) to burn CPU
		// deterministically without allocating or touching memory.
		x = x*6364136223846793005 + 1442695040888963407
	}
	h.sink = x
	return nil
}

// BenchmarkParallelPointerRelayWithWork mirrors BenchmarkParallelPointerRelay
// but gives each handler a fixed amount of CPU work per event. This shows
// whether the worker fan-out pays for itself once handlers are non-trivial,
// unlike the dispatch-only benchmark where parallelism is pure overhead.
//
//	go test -bench=BenchmarkParallelPointerRelayWithWork -benchmem -run=^$ ./...
func BenchmarkParallelPointerRelayWithWork(b *testing.B) {
	// ~a few microseconds of CPU per event; large enough to dominate the
	// dispatch/channel overhead so added workers translate into speedup.
	const workIterations = 5000
	const batchSize = 500

	cases := []struct {
		name        string
		parallelism int
	}{
		{"P1", 1},
		{"P2", 2},
		{"P4", 4},
		{"P8", 8},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			store := &benchPointerStore{}
			incStore := newBenchIncrementIDStore()

			entityIDs := make([]uuid.UUID, 100)
			for i := range entityIDs {
				entityIDs[i] = uuid.Must(uuid.NewV4())
			}
			const totalEntities = 100
			const eventsPerEntity = 200
			domainEvents := make([]DomainEvent, 0, totalEntities*eventsPerEntity)
			for i := 0; i < totalEntities*eventsPerEntity; i++ {
				ev := newBenchEvent(int64(i + 1)).(*benchDomainEvent)
				ev.entityID = entityIDs[i%totalEntities]
				domainEvents = append(domainEvents, ev)
			}
			if err := store.Append(context.Background(), domainEvents...); err != nil {
				b.Fatalf("seed: %v", err)
			}

			relay := NewPointerHandlerRelay(
				"bench-parallel-work",
				store, incStore,
				func(WorkerContext) Handler { return &benchWorkHandler{iterations: workIterations} },
				WithBatchSize(batchSize),
				WithParallelism(tc.parallelism),
			)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := incStore.SetIncrementID(
					context.Background(),
					"bench-parallel-work",
					incStore.vals["bench-parallel-work"],
					0,
				); err != nil {
					b.Fatalf("reset cursor: %v", err)
				}
				if err := drainPointerRelay(relay, nil); err != nil {
					b.Fatalf("drain: %v", err)
				}
			}
		})
	}
}

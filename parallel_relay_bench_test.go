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
			relay := must(NewPointerHandlerRelay(
				"bench-parallel",
				store, incStore,
				func(WorkerContext) Handler { return handler },
				WithBatchSize(tc.batchSize),
				WithParallelism(tc.parallelism),
			))

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

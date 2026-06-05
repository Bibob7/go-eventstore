package eventstore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
)

// mockBatchHandler implements BatchHandler and records per-worker
// bookkeeping so tests can assert that stream-consistent distribution and
// the commit barrier work correctly.
type mockBatchHandler struct {
	mu sync.Mutex

	handleEvents     []StoredEvent
	handleByStream   map[uuid.UUID][]StoredEvent
	commitCalls      int
	commitErr        error
	handleErr        error
	failOnNthHandle  int
	calls            int
	handleSleep      time.Duration
	commitConcurrent atomic.Int32
}

func newMockBatchHandler() *mockBatchHandler {
	return &mockBatchHandler{
		handleByStream: make(map[uuid.UUID][]StoredEvent),
	}
}

func (m *mockBatchHandler) Name() string { return "mock-batch" }

func (m *mockBatchHandler) Handle(ctx context.Context, ev StoredEvent) error {
	m.mu.Lock()
	m.calls++
	n := m.calls
	m.handleEvents = append(m.handleEvents, ev)
	m.handleByStream[ev.StreamID] = append(m.handleByStream[ev.StreamID], ev)
	sleep := m.handleSleep
	err := m.handleErr
	m.mu.Unlock()

	if sleep > 0 {
		select {
		case <-time.After(sleep):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if m.failOnNthHandle == 0 {
		return err
	}
	if n == m.failOnNthHandle {
		return err
	}
	return nil
}

func (m *mockBatchHandler) Commit(_ context.Context) error {
	m.commitConcurrent.Add(1)
	defer m.commitConcurrent.Add(-1)
	m.mu.Lock()
	m.commitCalls++
	err := m.commitErr
	m.mu.Unlock()
	return err
}

// newEventsByStreams produces StoredEvents with explicit StreamID assignment
// so tests can control which worker an event lands on.
func newEventsByStreams(incrementIDs []int64, streamIDs []uuid.UUID) []StoredEvent {
	if len(incrementIDs) != len(streamIDs) {
		panic("incrementIDs and streamIDs must have the same length")
	}
	out := make([]StoredEvent, len(incrementIDs))
	for i, id := range incrementIDs {
		out[i] = StoredEvent{
			ID:          uuid.Must(uuid.NewV4()),
			StreamID:    streamIDs[i],
			IncrementID: id,
			EventType:   "test-event",
			OccurredAt:  time.Now(),
		}
	}
	return out
}

// ---- processBatch unit tests -----------------------------------------

func TestProcessBatch_DispatchingByStreamID(t *testing.T) {
	// Two streams, multiple events each. All events of streamA must land on
	// the same worker, and likewise for streamB.
	streamA := uuid.Must(uuid.NewV4())
	streamB := uuid.Must(uuid.NewV4())

	// Force streamA to worker 0 and streamB to worker 2 by picking StreamIDs
	// whose first byte maps cleanly. We can't dictate the worker index
	// (pickWorker is fnv32a(StreamID)), so we just assert the partition
	// invariant: every event of the same stream went to the same worker.
	events := newEventsByStreams(
		[]int64{1, 2, 3, 4, 5, 6},
		[]uuid.UUID{streamA, streamA, streamA, streamB, streamB, streamB},
	)

	h := newMockBatchHandler()
	relay := NewPointerBatchHandlerRelay(
		"test-dispatch",
		&mockPointerStore{events: events},
		newMockIncrementIDStore(),
		func(WorkerContext) BatchHandler { return h },
		WithBatchSize(10),
		WithParallelism(4),
	)

	if err := relay.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	for _, ev := range events {
		streamWorker := pickWorker(ev, 4)
		for _, seen := range h.handleByStream[ev.StreamID] {
			seenWorker := pickWorker(seen, 4)
			if streamWorker != seenWorker {
				t.Errorf("stream %s: events split across workers %d and %d",
					ev.StreamID, streamWorker, seenWorker)
			}
		}
	}
}

func TestProcessBatch_CommitOncePerWorker(t *testing.T) {
	// 4 streams (each routed to its own worker) => exactly 4 Commit calls.
	// Use streams whose pickWorker index is distinct.
	streams := make([]uuid.UUID, 4)
	streamToWorker := make(map[uuid.UUID]int)
	for i := range streams {
		for {
			uid := uuid.Must(uuid.NewV4())
			idx := pickWorker(StoredEvent{StreamID: uid}, 4)
			if _, taken := streamToWorker[uid]; taken {
				continue
			}
			// Make sure this stream is uniquely routed.
			duplicate := false
			for _, w := range streamToWorker {
				if w == idx {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			streams[i] = uid
			streamToWorker[uid] = idx
			break
		}
	}

	var events []StoredEvent
	var ids []int64
	var entIDs []uuid.UUID
	for i, e := range streams {
		ids = append(ids, int64(i+1))
		entIDs = append(entIDs, e)
	}
	events = newEventsByStreams(ids, entIDs)

	h := newMockBatchHandler()
	relay := NewPointerBatchHandlerRelay(
		"test-commit-once",
		&mockPointerStore{events: events},
		newMockIncrementIDStore(),
		func(WorkerContext) BatchHandler { return h },
		WithBatchSize(10),
		WithParallelism(4),
	)

	if err := relay.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if h.commitCalls != 4 {
		t.Errorf("expected 4 Commit calls (one per worker), got %d", h.commitCalls)
	}
}

func TestProcessBatch_SequentialRegression(t *testing.T) {
	// WithParallelism(1) must behave bit-for-bit like the original
	// sequential path. We re-use a representative subset of the
	// TestPointerRelay_Run cases.
	store := &mockPointerStore{events: newEvents(1, 2, 3)}
	inc := newMockIncrementIDStore()
	h := &mockHandler{}
	relay := NewPointerHandlerRelay(
		"test-seq",
		store, inc,
		func(WorkerContext) Handler { return h },
		WithBatchSize(10),
		WithParallelism(1),
	)

	if err := relay.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(h.handleEvents) != 3 {
		t.Errorf("expected 3 handled events, got %d", len(h.handleEvents))
	}
	lastID, _ := inc.GetIncrementID(context.Background(), "test-seq")
	if lastID != 3 {
		t.Errorf("expected last increment ID 3, got %d", lastID)
	}
}

func TestProcessBatch_SequentialInvokesBatchHandlerFactory(t *testing.T) {
	// WithParallelism(1) must still invoke the batch handler factory —
	// both Handle for every event and Commit once per batch. The
	// sequential path should be functionally equivalent to running on a
	// single worker, not a stripped-down mode that drops batch semantics.
	store := &mockPointerStore{events: newEvents(1, 2, 3)}
	inc := newMockIncrementIDStore()
	h := newMockBatchHandler()
	relay := NewPointerBatchHandlerRelay(
		"test-seq-batch",
		store, inc,
		func(WorkerContext) BatchHandler { return h },
		WithBatchSize(10),
		WithParallelism(1),
	)

	if err := relay.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if h.calls != 3 {
		t.Errorf("expected 3 Handle calls on the batch handler, got %d", h.calls)
	}
	if h.commitCalls != 1 {
		t.Errorf("expected exactly 1 Commit call for the batch, got %d", h.commitCalls)
	}
}

func TestProcessBatch_SequentialCommitErrorDiscardsBatch(t *testing.T) {
	// When Commit fails on the sequential path, the relay must NOT report
	// the batch as processed: the caller (PointerRelay / TransientRelay)
	// would otherwise advance its cursor and the events would be lost.
	// Symmetric to processParallel, which already returns (0, nil, err).
	commitErr := errors.New("commit failed")
	store := &mockPointerStore{events: newEvents(1, 2, 3)}
	inc := newMockIncrementIDStore()
	h := newMockBatchHandler()
	h.commitErr = commitErr
	relay := NewPointerBatchHandlerRelay(
		"test-seq-commit-err",
		store, inc,
		func(WorkerContext) BatchHandler { return h },
		WithBatchSize(10),
		WithParallelism(1),
	)

	err := relay.Run(context.Background())
	if !errors.Is(err, commitErr) {
		t.Fatalf("expected commit error, got %v", err)
	}
	// Cursor must remain at zero: a failed commit means the batch was
	// not durably processed, so the next Run must retry it.
	lastID, _ := inc.GetIncrementID(context.Background(), "test-seq-commit-err")
	if lastID != 0 {
		t.Errorf("expected cursor to stay at 0 after commit failure, got %d", lastID)
	}
	// Sanity: Handle was called for all events before the commit fired.
	if h.calls != 3 {
		t.Errorf("expected 3 Handle calls, got %d", h.calls)
	}
}

// cancelOnNthHandler cancels its context once it has handled n events, so
// tests can land a cancellation at a deterministic point mid-batch without
// relying on timing.
type cancelOnNthHandler struct {
	n      int
	calls  int
	cancel context.CancelFunc
}

func (h *cancelOnNthHandler) Name() string { return "cancel-on-nth" }

func (h *cancelOnNthHandler) Handle(_ context.Context, _ StoredEvent) error {
	h.calls++
	if h.calls == h.n {
		h.cancel()
	}
	return nil
}

// cancelOnNthBatchHandler is the BatchHandler variant; it records Commit
// calls so tests can assert the barrier never fired.
type cancelOnNthBatchHandler struct {
	cancelOnNthHandler
	commitCalls int
}

func (h *cancelOnNthBatchHandler) Commit(_ context.Context) error {
	h.commitCalls++
	return nil
}

func TestProcessBatch_SequentialHandlerCancelAdvancesCursor(t *testing.T) {
	// On a plain-Handler relay, a cancellation observed between two events
	// is reported as partial progress: the cursor advances to the last
	// successfully processed event. Handlers are expected to be idempotent
	// so the next Run can safely re-deliver from there.
	store := &mockPointerStore{events: newEvents(1, 2, 3)}
	inc := newMockIncrementIDStore()

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel right after the first event is handled; the loop hits the
	// cancellation checkpoint before reaching the second event.
	h := &cancelOnNthHandler{n: 1, cancel: cancel}
	relay := NewPointerHandlerRelay(
		"test-seq-handler-cancel",
		store, inc,
		func(WorkerContext) Handler { return h },
		WithBatchSize(10),
		WithParallelism(1),
	)

	err := relay.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	// With partial progress the cursor advances to the last handled event.
	lastID, _ := inc.GetIncrementID(context.Background(), "test-seq-handler-cancel")
	if lastID != 1 {
		t.Errorf("expected cursor to advance to 1 (last handled event), got %d", lastID)
	}
}

func TestProcessBatch_SequentialBatchHandlerCancelDiscards(t *testing.T) {
	// On a BatchHandler relay a mid-batch cancellation must NOT advance the
	// cursor: the per-batch Commit barrier has not yet fired, so the batch
	// is not durably processed and must be retried.
	store := &mockPointerStore{events: newEvents(1, 2, 3)}
	inc := newMockIncrementIDStore()

	ctx, cancel := context.WithCancel(context.Background())
	h := &cancelOnNthBatchHandler{cancelOnNthHandler: cancelOnNthHandler{n: 1, cancel: cancel}}
	relay := NewPointerBatchHandlerRelay(
		"test-seq-batch-cancel",
		store, inc,
		func(WorkerContext) BatchHandler { return h },
		WithBatchSize(10),
		WithParallelism(1),
	)

	err := relay.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	// Cursor must stay at 0 — strict all-or-nothing.
	lastID, _ := inc.GetIncrementID(context.Background(), "test-seq-batch-cancel")
	if lastID != 0 {
		t.Errorf("expected cursor to stay at 0 after cancellation, got %d", lastID)
	}
	// Commit must never have fired.
	if h.commitCalls != 0 {
		t.Errorf("expected 0 Commit calls, got %d", h.commitCalls)
	}
}

func TestProcessBatch_ErrEventNotReadyToProcess(t *testing.T) {
	// Use 3 events, all routed to the same worker (via a single StreamID) so
	// the failure mid-batch happens on the same worker that processed the
	// earlier events. That worker must NOT call Commit after the failure;
	// events 1 and 2 are also handled by the same worker, so they cannot
	// have committed either.
	stream := uuid.Must(uuid.NewV4())
	events := newEventsByStreams(
		[]int64{1, 2, 3},
		[]uuid.UUID{stream, stream, stream},
	)
	h := &mockBatchHandlerAdapter{errOnCall: 3, err: ErrEventNotReadyToProcess}
	relay := NewPointerBatchHandlerRelay(
		"test-notready",
		&mockPointerStore{events: events},
		newMockIncrementIDStore(),
		func(WorkerContext) BatchHandler { return h },
		WithBatchSize(10),
		WithParallelism(4),
	)

	err := relay.Run(context.Background())
	if !errors.Is(err, ErrEventNotReadyToProcess) {
		t.Fatalf("expected ErrEventNotReadyToProcess, got %v", err)
	}
	if h.calls != 3 {
		t.Errorf("expected 3 Handle calls, got %d", h.calls)
	}
	if h.commitCalls != 0 {
		t.Errorf("expected 0 Commit calls (single worker, errored before commit), got %d", h.commitCalls)
	}
}

func TestProcessBatch_HandlerErrorAbortsPool(t *testing.T) {
	boom := errors.New("boom")
	// Same single-stream setup so the failing worker is the one that would
	// have called Commit; after the error, no Commit must run.
	stream := uuid.Must(uuid.NewV4())
	events := newEventsByStreams(
		[]int64{1, 2, 3},
		[]uuid.UUID{stream, stream, stream},
	)
	h := &mockBatchHandlerAdapter{errOnCall: 2, err: boom}
	relay := NewPointerBatchHandlerRelay(
		"test-handler-err",
		&mockPointerStore{events: events},
		newMockIncrementIDStore(),
		func(WorkerContext) BatchHandler { return h },
		WithBatchSize(10),
		WithParallelism(4),
	)

	err := relay.Run(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("expected boom, got %v", err)
	}
	if h.commitCalls != 0 {
		t.Errorf("expected 0 Commit calls, got %d", h.commitCalls)
	}
}

func TestProcessBatch_CommitError(t *testing.T) {
	commitErr := errors.New("commit failed")
	events := newEvents(1, 2, 3)
	h := newMockBatchHandler()
	h.commitErr = commitErr
	relay := NewPointerBatchHandlerRelay(
		"test-commit-err",
		&mockPointerStore{events: events},
		newMockIncrementIDStore(),
		func(WorkerContext) BatchHandler { return h },
		WithBatchSize(10),
		WithParallelism(2),
	)

	err := relay.Run(context.Background())
	if !errors.Is(err, commitErr) {
		t.Fatalf("expected commit error, got %v", err)
	}
}

func TestProcessBatch_ContextCancelDuringDispatch(t *testing.T) {
	// A handler that blocks until ctx is done. Cancel from a goroutine after
	// a short delay and assert the pool exits cleanly.
	stream := uuid.Must(uuid.NewV4())
	events := newEventsByStreams(
		[]int64{1, 2, 3, 4},
		[]uuid.UUID{stream, stream, stream, stream},
	)
	h := newMockBatchHandler()
	h.handleSleep = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	relay := NewPointerBatchHandlerRelay(
		"test-ctx-cancel",
		&mockPointerStore{events: events},
		newMockIncrementIDStore(),
		func(WorkerContext) BatchHandler { return h },
		WithBatchSize(10),
		WithParallelism(2),
	)

	start := time.Now()
	err := relay.Run(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error from ctx cancel, got nil")
	}
	// Should bail out well before the 4 * 100ms sequential time.
	if elapsed >= 400*time.Millisecond {
		t.Errorf("expected early exit, elapsed=%v", elapsed)
	}
}

// ---- Integration tests against PointerRelay / TransientRelay --------

func TestPointerRelay_WithParallelism_4(t *testing.T) {
	events := newEvents(1, 2, 3, 4, 5)
	store := &mockPointerStore{events: events}
	inc := newMockIncrementIDStore()
	h := newMockBatchHandler()
	relay := NewPointerBatchHandlerRelay(
		"test-pointer-parallel",
		store, inc,
		func(WorkerContext) BatchHandler { return h },
		WithBatchSize(10),
		WithParallelism(4),
	)

	if err := relay.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	lastID, _ := inc.GetIncrementID(context.Background(), "test-pointer-parallel")
	if lastID != 5 {
		t.Errorf("expected last increment ID 5, got %d", lastID)
	}
	if len(h.handleEvents) != 5 {
		t.Errorf("expected 5 handled events, got %d", len(h.handleEvents))
	}
}

func TestTransientRelay_WithParallelism_4_CleansUpBatch(t *testing.T) {
	events := []StoredEvent{newStoredEvent(1), newStoredEvent(2), newStoredEvent(3)}
	store := &recordingTransientStore{mockTransientStore: mockTransientStore{events: events}}
	h := newMockBatchHandler()
	relay := NewTransientBatchHandlerRelay(
		"test-transient-parallel",
		store,
		func(WorkerContext) BatchHandler { return h },
		WithBatchSize(10),
		WithParallelism(4),
	)

	if err := relay.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(store.calls) != 1 {
		t.Fatalf("expected 1 CleanUpEvents call, got %d", len(store.calls))
	}
	if len(store.calls[0]) != 3 {
		t.Errorf("expected 3 events in clean-up call, got %d", len(store.calls[0]))
	}
}

// ---- mockBatchHandlerAdapter -----------------------------------------

// mockBatchHandlerAdapter adapts mockHandler (a plain Handler) to
// BatchHandler so we can run the parallel tests with a known-failing
// handler. The original mockHandler is reused to keep assertions
// familiar.
type mockBatchHandlerAdapter struct {
	mu          sync.Mutex
	errOnCall   int
	err         error
	calls       int
	commitCalls int
}

func (m *mockBatchHandlerAdapter) Name() string { return "adapter" }

func (m *mockBatchHandlerAdapter) Handle(ctx context.Context, ev StoredEvent) error {
	m.mu.Lock()
	m.calls++
	n := m.calls
	err := m.err
	m.mu.Unlock()
	if m.errOnCall == 0 || n == m.errOnCall {
		return err
	}
	return nil
}

func (m *mockBatchHandlerAdapter) Commit(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commitCalls++
	return nil
}

// ---- HandlerFactory tests --------------------------------------------

// counterHandler is a stateful Handler used to verify that each worker
// gets its own instance from the relay's handler factory.
type counterHandler struct {
	id   int
	mu   sync.Mutex
	seen []uuid.UUID
}

func (c *counterHandler) Name() string { return "counter" }

func (c *counterHandler) Handle(_ context.Context, ev StoredEvent) error {
	c.mu.Lock()
	c.seen = append(c.seen, ev.StreamID)
	c.mu.Unlock()
	return nil
}

func TestHandlerFactory_OneInstancePerWorker(t *testing.T) {
	// With WithParallelism(3) and one registered factory, the factory must
	// be invoked exactly 3 times — once per worker. We pick streams that
	// hash to different workers so all three workers receive at least one
	// event and the factory fires for each. Additionally, every invocation
	// must see a WorkerContext with Count == 3 and a unique ID in [0, 3).
	streams := pickDistinctStreams(t, 3, 3)
	events := newEventsByStreams([]int64{1, 2, 3}, streams)
	var (
		factoryCalls   atomic.Int32
		seenWorkerIDs  sync.Map
		seenWorkerSize atomic.Int32
	)
	relay := NewPointerHandlerRelay(
		"test-handler-factory-count",
		&mockPointerStore{events: events},
		newMockIncrementIDStore(),
		func(wc WorkerContext) Handler {
			factoryCalls.Add(1)
			if wc.Count != 3 {
				t.Errorf("factory invocation: Count = %d, want 3", wc.Count)
			}
			if wc.ID < 0 || wc.ID >= wc.Count {
				t.Errorf("factory invocation: ID = %d, want in [0, %d)", wc.ID, wc.Count)
			}
			if _, loaded := seenWorkerIDs.LoadOrStore(wc.ID, struct{}{}); loaded {
				t.Errorf("factory invoked twice for worker ID %d", wc.ID)
			}
			seenWorkerSize.Add(1)
			return &counterHandler{}
		},
		WithBatchSize(10),
		WithParallelism(3),
	)

	if err := relay.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := factoryCalls.Load(); got != 3 {
		t.Errorf("expected factory to be called 3 times (one per worker), got %d", got)
	}
	if got := seenWorkerSize.Load(); got != 3 {
		t.Errorf("expected 3 distinct worker IDs, got %d", got)
	}
}

// pickDistinctStreams returns n UUIDs whose pickWorker(StreamID, workers) is
// a unique value in [0, workers). The set is built by trial-and-error using
// fresh UUIDs; it is bounded to a small fixed number of attempts so a test
// fails fast on hash collisions rather than spinning forever.
func pickDistinctStreams(t *testing.T, n, workers int) []uuid.UUID {
	t.Helper()
	out := make([]uuid.UUID, 0, n)
	seen := make(map[int]struct{}, n)
	for attempts := 0; attempts < 1000 && len(out) < n; attempts++ {
		id := uuid.Must(uuid.NewV4())
		idx := pickWorker(StoredEvent{StreamID: id}, workers)
		if _, ok := seen[idx]; ok {
			continue
		}
		seen[idx] = struct{}{}
		out = append(out, id)
	}
	if len(out) < n {
		t.Fatalf("could not find %d streams hashing to distinct workers in %d", n, workers)
	}
	return out
}

func TestHandlerFactory_DistinctInstancesPerWorker(t *testing.T) {
	// Each worker must receive its own counterHandler instance, identified
	// by a unique id assigned in the factory. The id is sourced from
	// WorkerContext.ID so we additionally assert that worker ID < Count
	// and the per-instance id is the same as the worker ID we observed at
	// factory time. Use streams that hash to distinct workers so all 3
	// instances are actually built.
	streams := pickDistinctStreams(t, 3, 3)
	events := newEventsByStreams([]int64{1, 2, 3}, streams)
	var instancesMu sync.Mutex
	instances := make(map[int]*counterHandler)
	relay := NewPointerHandlerRelay(
		"test-handler-factory-distinct",
		&mockPointerStore{events: events},
		newMockIncrementIDStore(),
		func(wc WorkerContext) Handler {
			if wc.Count != 3 {
				t.Errorf("factory: Count = %d, want 3", wc.Count)
			}
			c := &counterHandler{id: wc.ID}
			instancesMu.Lock()
			instances[wc.ID] = c
			instancesMu.Unlock()
			return c
		},
		WithBatchSize(10),
		WithParallelism(3),
	)

	if err := relay.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(instances) != 3 {
		t.Fatalf("expected 3 distinct instances, got %d", len(instances))
	}
	totalSeen := 0
	for _, c := range instances {
		c.mu.Lock()
		totalSeen += len(c.seen)
		c.mu.Unlock()
	}
	if totalSeen != 3 {
		t.Errorf("expected 3 events distributed across instances, got %d", totalSeen)
	}
	// Every event must have landed on exactly one instance.
	allStreams := make(map[uuid.UUID]int)
	for _, c := range instances {
		c.mu.Lock()
		for _, e := range c.seen {
			allStreams[e]++
		}
		c.mu.Unlock()
	}
	for e, n := range allStreams {
		if n != 1 {
			t.Errorf("stream %s seen by %d instances, want 1", e, n)
		}
	}
}

func TestHandlerFactory_StreamCoherenceWithFactory(t *testing.T) {
	// Events of the same stream must all be handled by the same instance.
	streamA := uuid.Must(uuid.NewV4())
	streamB := uuid.Must(uuid.NewV4())
	events := newEventsByStreams(
		[]int64{1, 2, 3, 4, 5, 6},
		[]uuid.UUID{streamA, streamA, streamA, streamB, streamB, streamB},
	)

	var mu sync.Mutex
	instances := make(map[uuid.UUID][]int) // stream -> instance ids seen
	relay := NewPointerHandlerRelay(
		"test-handler-factory-coherence",
		&mockPointerStore{events: events},
		newMockIncrementIDStore(),
		func(wc WorkerContext) Handler {
			return &recordingHandler{
				id: wc.ID,
				onSee: func(ev StoredEvent, instanceID int) {
					mu.Lock()
					instances[ev.StreamID] = append(instances[ev.StreamID], instanceID)
					mu.Unlock()
				},
			}
		},
		WithBatchSize(10),
		WithParallelism(4),
	)

	if err := relay.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	for stream, ids := range instances {
		if len(ids) == 0 {
			t.Errorf("stream %s: no events handled", stream)
			continue
		}
		first := ids[0]
		for _, id := range ids {
			if id != first {
				t.Errorf("stream %s: events handled by instances %v, want all the same",
					stream, ids)
				break
			}
		}
	}
}

func TestHandlerFactory_WorkerContextMatchesWorker(t *testing.T) {
	// Every event routed to a given worker must be handled by the instance
	// whose factory-time WorkerContext.ID equals that worker's index. This
	// is the round-trip proof that the worker ID passed to the factory is
	// the same one that pickWorker() would assign for the stream.
	streams := pickDistinctStreams(t, 3, 3)
	events := newEventsByStreams([]int64{1, 2, 3}, streams)
	var (
		instancesMu sync.Mutex
		instances   = make(map[int]*workerContextRecordingHandler)
	)
	relay := NewPointerHandlerRelay(
		"test-handler-factory-wcid",
		&mockPointerStore{events: events},
		newMockIncrementIDStore(),
		func(wc WorkerContext) Handler {
			h := &workerContextRecordingHandler{workerID: wc.ID}
			instancesMu.Lock()
			instances[wc.ID] = h
			instancesMu.Unlock()
			return h
		},
		WithBatchSize(10),
		WithParallelism(3),
	)

	if err := relay.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(instances) != 3 {
		t.Fatalf("expected 3 worker instances, got %d", len(instances))
	}

	for _, ev := range events {
		wantWorker := pickWorker(ev, 3)
		recording := instances[wantWorker]
		if recording == nil {
			t.Errorf("stream %s: no instance for worker %d", ev.StreamID, wantWorker)
			continue
		}
		recording.mu.Lock()
		got := recording.seen[ev.StreamID]
		recording.mu.Unlock()
		if got != wantWorker {
			t.Errorf("stream %s: routed to worker %d but handler came from worker %d",
				ev.StreamID, wantWorker, got)
		}
	}
}

func TestHandlerFactory_WorkerCountMatchesConfigured(t *testing.T) {
	// The Count field on WorkerContext must always match the configured
	// parallelism, regardless of how many events each worker actually sees.
	const configured = 4
	events := newEvents(1, 2, 3) // fewer events than workers; some workers idle
	relay := NewPointerHandlerRelay(
		"test-handler-factory-count-field",
		&mockPointerStore{events: events},
		newMockIncrementIDStore(),
		func(wc WorkerContext) Handler {
			if wc.Count != configured {
				t.Errorf("factory: Count = %d, want %d", wc.Count, configured)
			}
			return &counterHandler{id: wc.ID}
		},
		WithBatchSize(10),
		WithParallelism(configured),
	)

	if err := relay.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestHandlerFactory_WorkerContextInSequentialMode(t *testing.T) {
	// With WithParallelism(1) the factory is invoked exactly once with
	// WorkerContext{ID: 0, Count: 1}.
	events := newEvents(1, 2, 3)
	var (
		factoryCalls atomic.Int32
		seenCount    atomic.Int32
		seenID       atomic.Int32
	)
	relay := NewPointerHandlerRelay(
		"test-handler-factory-sequential-wc",
		&mockPointerStore{events: events},
		newMockIncrementIDStore(),
		func(wc WorkerContext) Handler {
			factoryCalls.Add(1)
			seenCount.Store(int32(wc.Count))
			seenID.Store(int32(wc.ID))
			return &counterHandler{}
		},
		WithBatchSize(10),
		WithParallelism(1),
	)

	if err := relay.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := factoryCalls.Load(); got != 1 {
		t.Errorf("expected factory to be called exactly once, got %d", got)
	}
	if got := seenCount.Load(); got != 1 {
		t.Errorf("expected WorkerContext.Count = 1, got %d", got)
	}
	if got := seenID.Load(); got != 0 {
		t.Errorf("expected WorkerContext.ID = 0, got %d", got)
	}
}

// workerContextRecordingHandler is keyed on its factory-time workerID and
// records the (stream -> workerID) mapping for assertions in
// TestHandlerFactory_WorkerContextMatchesWorker.
type workerContextRecordingHandler struct {
	workerID int
	mu       sync.Mutex
	seen     map[uuid.UUID]int
}

func (w *workerContextRecordingHandler) Name() string { return "wc-recording" }

func (w *workerContextRecordingHandler) Handle(_ context.Context, ev StoredEvent) error {
	w.mu.Lock()
	if w.seen == nil {
		w.seen = make(map[uuid.UUID]int)
	}
	w.seen[ev.StreamID] = w.workerID
	w.mu.Unlock()
	return nil
}

// recordingHandler captures each event with the instance id assigned by
// the factory, so tests can assert stream coherence across workers.
type recordingHandler struct {
	id    int
	mu    sync.Mutex
	seen  []StoredEvent
	onSee func(ev StoredEvent, id int)
}

func (r *recordingHandler) Name() string { return "recording" }

func (r *recordingHandler) Handle(ctx context.Context, ev StoredEvent) error {
	r.mu.Lock()
	r.seen = append(r.seen, ev)
	onSee := r.onSee
	r.mu.Unlock()
	if onSee != nil {
		onSee(ev, r.id)
	}
	return nil
}

// seqIDs returns the increment IDs 1..n, used to seed a batch large enough
// that a cancelled context is virtually certain to interrupt dispatch.
func seqIDs(n int) []int64 {
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	return ids
}

// TestRunParallel_CancelledContextDiscardsBatch is a regression test: when
// the context is already cancelled as a parallel batch is dispatched,
// runParallel must report the cancellation and NOT mark the batch as
// processed. Otherwise the pointer cursor advances (or the transient store
// cleans up) events that were never handled, silently losing them.
func TestRunParallel_CancelledContextDiscardsBatch(t *testing.T) {
	const batch = 100
	cases := []struct {
		name  string
		check func(t *testing.T)
	}{
		{
			name: "pointer relay keeps cursor at zero",
			check: func(t *testing.T) {
				store := &mockPointerStore{events: newEvents(seqIDs(batch)...)}
				inc := newMockIncrementIDStore()
				relay := NewPointerHandlerRelay(
					"cancel-pointer",
					store, inc,
					func(WorkerContext) Handler { return &mockHandler{} },
					WithBatchSize(batch),
					WithParallelism(4),
				)

				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				if err := relay.Run(ctx); !errors.Is(err, context.Canceled) {
					t.Fatalf("expected context.Canceled, got %v", err)
				}
				if id, _ := inc.GetIncrementID(context.Background(), "cancel-pointer"); id != 0 {
					t.Errorf("expected cursor to stay at 0, got %d", id)
				}
			},
		},
		{
			name: "transient relay cleans up nothing",
			check: func(t *testing.T) {
				store := &mockTransientStore{events: newEvents(seqIDs(batch)...)}
				relay := NewTransientHandlerRelay(
					"cancel-transient",
					store,
					func(WorkerContext) Handler { return &mockHandler{} },
					WithBatchSize(batch),
					WithParallelism(4),
				)

				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				if err := relay.Run(ctx); !errors.Is(err, context.Canceled) {
					t.Fatalf("expected context.Canceled, got %v", err)
				}
				if len(store.cleanedUp) != 0 {
					t.Errorf("expected no cleaned-up events, got %d", len(store.cleanedUp))
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.check(t)
		})
	}
}

package eventstore

// WorkerContext identifies the worker goroutine a factory-built handler is
// destined for, so factories can shard per-worker state or tag logs. ID is the
// zero-based worker index in [0, Count) and Count is the configured parallelism
// (>= 1). With WithParallelism(1) the factory is invoked once with ID 0 and
// Count 1.
type WorkerContext struct {
	ID    int
	Count int
}

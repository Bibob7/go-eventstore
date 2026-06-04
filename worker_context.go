package eventstore

// WorkerContext identifies which worker goroutine a factory-built
// handler is destined for. It is passed to factories registered via
// RegisterHandlerFactory and RegisterBatchHandler.
//
//	ID is the zero-based worker index in [0, Count).
//	Count is the configured parallelism (>= 1).
//
// Factories may use these to:
//
//   - return a pre-built per-worker instance from a map or slice
//   - tag log output, metrics, or error messages with the worker ID
//   - shard external resources (e.g. one AMQP channel per worker)
//
// In sequential runs (WithParallelism(1)) the factory is invoked
// exactly once with ID == 0 and Count == 1. A given worker index is
// stable for the lifetime of a single Run: events routed to that
// worker always see the same Handler instance the factory produced.
type WorkerContext struct {
	ID    int
	Count int
}

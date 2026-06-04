package eventstore

import "time"

// relayConfig holds all options collected during NewPointerRelay construction.
// It is separate from pointerRelay so that decorator-specific settings are not
// stored on the relay itself.
type relayConfig struct {
	batchSize             int
	handleDelay           time.Duration
	batchDelay            time.Duration
	conditionalBatchDelay time.Duration
	parallelism           int
}

// RelayOption is a functional option for configuring a PointerRelay.
type RelayOption func(*relayConfig)

// WithBatchSize sets the maximum number of events fetched per relay run.
// Defaults to DefaultBatchSize.
func WithBatchSize(batchSize int) RelayOption {
	return func(c *relayConfig) {
		c.batchSize = batchSize
	}
}

// WithHandleDelay inserts a pause between processing individual events within a batch.
// Useful for rate-limiting or giving downstream systems time to react.
func WithHandleDelay(delay time.Duration) RelayOption {
	return func(c *relayConfig) {
		c.handleDelay = delay
	}
}

// WithBatchDelay sets an unconditional delay between every relay run.
// The relay waits this duration after each batch, regardless of the result.
func WithBatchDelay(d time.Duration) RelayOption {
	return func(c *relayConfig) {
		c.batchDelay = d
	}
}

// WithConditionalBatchDelay sets a delay between relay runs when ErrEventNotReadyToProcess
// is returned. The relay waits this duration before the next run. Defaults to DefaultWaitTime.
func WithConditionalBatchDelay(d time.Duration) RelayOption {
	return func(c *relayConfig) {
		c.conditionalBatchDelay = d
	}
}

// WithParallelism runs the relay across n worker goroutines partitioned by
// the event's EntityID (fnv32a(EntityID) % n). All events of a given
// aggregate are processed sequentially on the same worker, preserving
// stream ordering. n must be >= 1; values < 1 are clamped to 1.
//
// When n > 1, factories registered via RegisterBatchHandler produce one
// BatchHandler per worker, and Commit is invoked once per worker per batch
// so per-batch work (e.g. AMQP publish) flushes atomically. Plain Handlers
// returned by RegisterHandlerFactory are auto-wrapped with a no-op Commit.
func WithParallelism(n int) RelayOption {
	return func(c *relayConfig) {
		if n < 1 {
			n = 1
		}
		c.parallelism = n
	}
}

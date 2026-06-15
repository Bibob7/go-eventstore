package eventstore

import "time"

// RelayOption configures a relay at construction time.
type RelayOption func(*relayConfig)

// WithBatchSize sets the maximum number of events fetched per relay run.
// Defaults to DefaultBatchSize.
func WithBatchSize(batchSize int) RelayOption {
	return func(c *relayConfig) {
		c.batchSize = batchSize
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

// WithParallelism runs the relay across n worker goroutines (values < 1 are
// clamped to 1). Each event is routed to a worker by the configured
// PartitionStrategy; the default keeps all events of a stream on one worker,
// preserving stream ordering. The factory produces one handler per worker, and
// the parallel path is always strict all-or-nothing.
func WithParallelism(n int) RelayOption {
	return func(c *relayConfig) {
		if n < 1 {
			n = 1
		}
		c.parallelism = n
	}
}

// WithPartitionStrategy replaces the strategy that routes events to workers in
// the parallel path. To process related events sequentially, route them to the
// same worker index. Passing nil falls back to DefaultPartitionStrategy.
func WithPartitionStrategy(s PartitionStrategy) RelayOption {
	return func(c *relayConfig) {
		if s == nil {
			s = DefaultPartitionStrategy
		}
		c.partitionStrategy = s
	}
}

package eventstore

import "time"

// RelayOption is a functional option for configuring a PointerRelay.
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

// WithParallelism runs the relay across n worker goroutines partitioned by
// the event's EntityID (fnv32a(EntityID) % n). All events of a given
// aggregate are processed sequentially on the same worker, preserving
// stream ordering. n must be >= 1; values < 1 are clamped to 1.
//
// On a BatchHandler relay, the factory produces one BatchHandler per
// worker and Commit fires once per worker per batch. On a plain-Handler
// relay, the factory produces one Handler per worker; the relay is strict
// all-or-nothing in the parallel path regardless of strategy (the
// per-EntityID partitioning makes partial per-worker progress unsafe
// to merge into a single cursor update).
func WithParallelism(n int) RelayOption {
	return func(c *relayConfig) {
		if n < 1 {
			n = 1
		}
		c.parallelism = n
	}
}

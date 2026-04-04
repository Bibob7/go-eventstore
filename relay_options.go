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

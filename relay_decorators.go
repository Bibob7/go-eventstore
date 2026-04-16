package eventstore

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// delayedRelay wraps a Relay and delays the next run when ErrEventNotReadyToProcess is returned.
type delayedRelay struct {
	relay    Relay
	waitTime time.Duration
}

func newDelayedRelay(relay Relay, waitTime time.Duration) Relay {
	if waitTime <= 0 {
		waitTime = DefaultWaitTime
	}
	return &delayedRelay{
		relay:    relay,
		waitTime: waitTime,
	}
}

func (r delayedRelay) Name() string {
	return r.relay.Name()
}

func (r delayedRelay) RegisterHandler(handler ...Handler) Relay {
	r.relay.RegisterHandler(handler...)
	return r
}

func (r delayedRelay) Run(ctx context.Context) error {
	err := r.relay.Run(ctx)
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrEventNotReadyToProcess) {
		slog.Info("Events relayed, delaying next batch because of ErrEventNotReadyToProcess", "name", r.relay.Name(), "wait_time", r.waitTime)
		timer := time.NewTimer(r.waitTime)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			slog.Debug("Context done, stopping delayed relay", "name", r.relay.Name())
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	}
	return err
}

// batchDelayedRelay wraps a Relay and adds an unconditional delay after every run.
type batchDelayedRelay struct {
	relay      Relay
	batchDelay time.Duration
}

func newBatchDelayedRelay(relay Relay, batchDelay time.Duration) Relay {
	return &batchDelayedRelay{
		relay:      relay,
		batchDelay: batchDelay,
	}
}

func (r batchDelayedRelay) Name() string {
	return r.relay.Name()
}

func (r batchDelayedRelay) RegisterHandler(handler ...Handler) Relay {
	r.relay.RegisterHandler(handler...)
	return r
}

func (r batchDelayedRelay) Run(ctx context.Context) error {
	err := r.relay.Run(ctx)
	slog.Debug("Delaying next batch", "name", r.relay.Name(), "batch_delay", r.batchDelay)
	timer := time.NewTimer(r.batchDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		slog.Debug("Context done, stopping batch delayed relay", "name", r.relay.Name())
		if err != nil {
			return err
		}
		return ctx.Err()
	case <-timer.C:
	}
	return err
}

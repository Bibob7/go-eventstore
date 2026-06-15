package eventstore

import "context"

// BatchHandler extends Handler with a per-batch Commit hook, letting a handler
// accumulate per-event work and flush it atomically (e.g. publish to AMQP) once
// the batch is processed. Used only on the BatchHandler relays
// (NewPointerBatchHandlerRelay / NewTransientBatchHandlerRelay), where Commit
// fires once per worker after Handle for all its events; any error discards the
// batch.
type BatchHandler interface {
	Handler
	Commit(ctx context.Context) error
}

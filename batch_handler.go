package eventstore

import "context"

// BatchHandler is an optional extension of Handler. It exposes a per-batch
// Commit hook so handlers can batch per-event work (e.g. enqueue to a channel)
// and flush atomically (e.g. publish to AMQP) once all events in the batch
// have been processed. This mirrors the MESSAGE_SYNC/MESSAGE_SYNC_ACK barrier
// in the PHP FollowEventStoreCommand.
//
// BatchHandlers are only ever used on a BatchHandler relay
// (NewPointerBatchHandlerRelay / NewTransientBatchHandlerRelay). The
// relay is strict all-or-nothing: Commit is invoked once per worker
// per batch, after that worker has finished Handle() for all its
// assigned events, and any error — Handle, Commit, or waitHandleDelay
// cancellation — discards the batch so the next Run retries it.
type BatchHandler interface {
	Handler
	Commit(ctx context.Context) error
}

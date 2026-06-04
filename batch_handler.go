package eventstore

import "context"

// BatchHandler is an optional extension of Handler. It exposes a per-batch
// Commit hook so handlers can batch per-event work (e.g. enqueue to a channel)
// and flush atomically (e.g. publish to AMQP) once all events in the batch
// have been processed. This mirrors the MESSAGE_SYNC/MESSAGE_SYNC_ACK barrier
// in the PHP FollowEventStoreCommand.
//
// A BatchHandler is required only when the relay is run with WithParallelism(n)
// where n > 1; in that mode Commit is invoked exactly once per worker per
// batch, after that worker has finished Handle() for all its assigned events.
// For sequential runs (n == 1) Commit is invoked once after the last event,
// and a plain Handler returned from a HandlerFactory is auto-wrapped via
// asBatchHandlers so its Commit is a no-op.
type BatchHandler interface {
	Handler
	Commit(ctx context.Context) error
}

// batchHandlerAdapter wraps a plain Handler as a BatchHandler whose Commit
// is a no-op. Used by processBatch when a user registers a non-batch handler
// via RegisterHandlerFactory, so a missing Commit never aborts the worker.
type batchHandlerAdapter struct {
	Handler
}

func (batchHandlerAdapter) Commit(_ context.Context) error { return nil }

// asBatchHandler converts a Handler to a BatchHandler. If the input already
// implements BatchHandler it is returned as-is; otherwise it is wrapped in a
// batchHandlerAdapter.
func asBatchHandler(h Handler) BatchHandler {
	if bh, ok := h.(BatchHandler); ok {
		return bh
	}
	return batchHandlerAdapter{Handler: h}
}

// asBatchHandlers maps a []Handler to []BatchHandler, applying asBatchHandler
// element-wise. The returned slice has the same length and order as the input.
func asBatchHandlers(handlers []Handler) []BatchHandler {
	out := make([]BatchHandler, len(handlers))
	for i, h := range handlers {
		out[i] = asBatchHandler(h)
	}
	return out
}

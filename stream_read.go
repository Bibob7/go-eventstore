package eventstore

import (
	"context"

	"github.com/gofrs/uuid/v5"
)

// ReadStreamAll reads a stream in full by paging through r.ReadStream in pages
// of pageSize, starting at fromVersion inclusive, until the stream is
// exhausted. It removes the truncation hazard of a single ReadStream call,
// whose result is capped at its limit.
//
// fromVersion < 0 is treated as 0 and pageSize < 1 falls back to
// DefaultBatchSize. Any ReadStream error is returned as-is. The whole stream is
// held in memory, so snapshot very long streams and resume from the snapshot's
// version.
func ReadStreamAll(ctx context.Context, r StreamReader, streamID uuid.UUID, fromVersion, pageSize int) ([]StoredEvent, error) {
	if fromVersion < 0 {
		fromVersion = 0
	}
	if pageSize < 1 {
		pageSize = DefaultBatchSize
	}

	var all []StoredEvent
	next := fromVersion
	for {
		page, err := r.ReadStream(ctx, streamID, next, pageSize)
		if err != nil {
			return nil, err
		}
		all = append(all, page...)
		// A short page means the stream is exhausted. Stream versions are
		// gapless and ascending, so resume right after the last one received.
		if len(page) < pageSize {
			return all, nil
		}
		next = page[len(page)-1].StreamVersion + 1
	}
}

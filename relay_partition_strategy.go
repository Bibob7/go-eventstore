package eventstore

import "hash/fnv"

// DefaultPartitionStrategy is used when no WithPartitionStrategy option is
// supplied.
var DefaultPartitionStrategy PartitionStrategy = HashStreamIDPartitionStrategy{}

// PartitionStrategy decides which worker index an event is routed to when the
// relay runs in parallel. To process related events sequentially (e.g. per
// stream), route them to the same worker index for a given workerCount.
//
// Implementations must be safe for concurrent use, as one value may be shared
// by several relays running at once.
type PartitionStrategy interface {
	// Partition returns the worker index in [0, workerCount) for the event.
	// workerCount is always >= 1.
	Partition(ev StoredEvent, workerCount int) int
}

// HashStreamIDPartitionStrategy hashes StreamID with fnv32a modulo workerCount,
// so the same stream always lands on the same worker and per-stream ordering is
// preserved. It is the DefaultPartitionStrategy.
type HashStreamIDPartitionStrategy struct{}

// Partition hashes ev.StreamID with fnv32a modulo workerCount, or returns 0
// when workerCount <= 1.
func (HashStreamIDPartitionStrategy) Partition(ev StoredEvent, workerCount int) int {
	if workerCount <= 1 {
		return 0
	}
	h := fnv.New32a()
	id := ev.StreamID
	_, _ = h.Write(id[:])
	return int(h.Sum32() % uint32(workerCount))
}

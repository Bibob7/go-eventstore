package eventstore

import "hash/fnv"

// DefaultPartitionStrategy is the PartitionStrategy used by a relay
// when no WithPartitionStrategy option is supplied. It preserves the
// historical hash-based partitioning so existing deployments see no
// behavior change.
var DefaultPartitionStrategy PartitionStrategy = HashStreamIDPartitionStrategy{}

// PartitionStrategy decides which worker index a given event is routed
// to when the relay runs in parallel.
//
// Any ordering guarantees depend on the chosen strategy: if some set of
// related events must be processed sequentially (e.g. per stream), the
// strategy should consistently route those related events to the same
// worker index for a given workerCount.
//
// Implementations must be safe for concurrent use: a single
// PartitionStrategy value (e.g. DefaultPartitionStrategy) may be shared by
// many relays running at the same time, so Partition can be called from
// several goroutines at once. Within a single relay Run, Partition is only
// ever invoked from one goroutine. (This does not make concurrent Runs of
// the same relay safe — see Relay.Run.)
type PartitionStrategy interface {
	// Partition returns the worker index in [0, workerCount) that the
	// event must be processed on. workerCount is always >= 1.
	Partition(ev StoredEvent, workerCount int) int
}

// HashStreamIDPartitionStrategy hashes StoredEvent.StreamID with fnv32a
// and reduces modulo workerCount. The same StreamID always lands on the
// same worker for a given workerCount, so per-stream ordering is
// preserved within a worker. It is the strategy used by
// DefaultPartitionStrategy and the behaviour the relay shipped with
// before the strategy was made pluggable.
type HashStreamIDPartitionStrategy struct{}

// Partition hashes ev.StreamID with fnv32a and returns the result modulo
// workerCount. With workerCount <= 1 the index is always 0.
func (HashStreamIDPartitionStrategy) Partition(ev StoredEvent, workerCount int) int {
	if workerCount <= 1 {
		return 0
	}
	h := fnv.New32a()
	id := ev.StreamID
	_, _ = h.Write(id[:])
	return int(h.Sum32() % uint32(workerCount))
}

# Go Event Store

A lightweight Go library for storing domain events in an append-only log and
processing them reliably. Events are appended with a monotonic position
(`IncrementID`) and consumed by **relays** that dispatch them to your handlers
with gap-safe, cursor-based ordering. Each relay tracks its own position, so
multiple independent relays can read the same events without coordinating.

It is a small building block, not a full event-sourcing framework: it gives you
a durable event log and reliable, ordered delivery to handlers — you decide what
to do with the events. That makes it a foundation for several patterns:

- **Event-driven processing & read models** — point several `PointerRelay`s at
  the same log, each with its own cursor, to build projections or trigger side
  effects. Events are retained, so relays can be added or replayed later.
- **Transactional outbox** — append events in the same transaction as your
  business data, then relay them to a message broker (a
  [polling publisher](https://microservices.io/patterns/data/polling-publisher.html)
  over the [transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html)).
  A `TransientRelay` deletes events after processing so the table does not grow.

The core module has no database dependency; a MySQL implementation is provided.

## Modules

| Module | Description |
|--------|-------------|
| `github.com/Bibob7/go-eventstore` | Core interfaces, pointer relay, and transient relay (no DB dependency) |
| `github.com/Bibob7/go-eventstore/integration/mysql` | MySQL implementation of `Store`, `PointerStore`, `TransientStore`, `CleanUpToStore`, and `IncrementIDStore` |

## Installation

```bash
# Core module only
go get github.com/Bibob7/go-eventstore

# With MySQL integration
go get github.com/Bibob7/go-eventstore/integration/mysql
```

## Database setup

Apply the DDL from [`integration/mysql/sql/mysql/schema.sql`](integration/mysql/sql/mysql/schema.sql) to create the required tables. Table names are configurable via `mysql.Config`:

```go
cfg := mysqlstore.Config{
    EventStoreTableName:  "event_store",
    IncrementIDTableName: "event_increment_id",
}
```

## Quickstart

### 1. Implement DomainEvent

```go
type OrderPlaced struct {
    id         uuid.UUID
    streamID   uuid.UUID
    occurredAt time.Time
    OrderID    string
}

func (e OrderPlaced) ID() uuid.UUID         { return e.id }
func (e OrderPlaced) StreamID() uuid.UUID   { return e.streamID }
func (e OrderPlaced) EventType() string     { return "OrderPlaced" }
func (e OrderPlaced) OccurredAt() time.Time { return e.occurredAt }
```

### 2. Append events

```go
store := mysqlstore.NewEventStore(db, "event_store")

err := store.Append(ctx, OrderPlaced{
    id:         uuid.Must(uuid.NewV4()),
    streamID:   orderID,
    occurredAt: time.Now(),
    OrderID:    "ord-123",
})
```

### 3. Implement a Handler

```go
type NotifyHandler struct{}

func (h *NotifyHandler) Name() string { return "notify-handler" }

func (h *NotifyHandler) Handle(ctx context.Context, event eventstore.StoredEvent) error {
    // decode event.Payload and process
    return nil
}
```

### 4. Run a PointerRelay

A `PointerRelay` tracks the last successfully processed `IncrementID` per relay so it can resume after a restart without reprocessing events.

```go
bundle := mysqlstore.NewEventStoreBundle(db, mysqlstore.Config{
    EventStoreTableName:  "event_store",
    IncrementIDTableName: "event_increment_id",
})

relay := eventstore.NewPointerHandlerRelay(
    "order-relay",
    bundle.EventStore,
    bundle.IncrementIDStore,
    func(eventstore.WorkerContext) eventstore.Handler {
        return &NotifyHandler{}
    },
    eventstore.WithBatchSize(50),
    eventstore.WithConditionalBatchDelay(2*time.Second),
)

// run in a loop, e.g. with a ticker
for {
    if err := relay.Run(ctx); err != nil {
        log.Println("relay error:", err)
    }
    time.Sleep(time.Second)
}
```

The relay is built around a *factory*: `func(eventstore.WorkerContext) eventstore.Handler`. The factory produces one handler instance per worker, so per-worker state is never shared by accident — see [Parallel relay](#parallel-relay-worker-pool).

> When using `WithParallelism(n)` with `n > 1`, the factory is invoked once
> per worker, so each worker gets its own private instance. If your factory
> returns the *same* instance to every worker, that instance is shared
> across goroutines and must be safe for concurrent use. Handlers that need
> per-batch atomicity (a flush after all of a worker's events are handled)
> implement `BatchHandler` and are registered via
> `NewPointerBatchHandlerRelay` / `NewTransientBatchHandlerRelay`.

### 5. Alternatively: TransientRelay

A `TransientRelay` deletes each event from the store after all handlers have processed it successfully. Useful when the outbox should not grow indefinitely and no separate cleanup job is desired.

```go
relay := eventstore.NewTransientHandlerRelay(
    "order-relay",
    bundle.EventStore, // EventStore also implements TransientStore
    func(eventstore.WorkerContext) eventstore.Handler {
        return &NotifyHandler{}
    },
    eventstore.WithBatchSize(50),
)
```

## Relay options

| Option | Purpose |
|---|---|
| `WithBatchSize(n)` | Max events fetched per `Run` (default `DefaultBatchSize`). |
| `WithBatchDelay(d)` | Unconditional delay between batches. |
| `WithConditionalBatchDelay(d)` | Delay applied only when a handler returns `ErrEventNotReadyToProcess`. |
| `WithParallelism(n)` | Run handler calls across `n` worker goroutines partitioned by `StreamID` (default `1`). See [Parallel relay](#parallel-relay-worker-pool) below. |

## Parallel relay (worker pool)

For high-throughput relays, `WithParallelism(n)` shards each batch across `n` worker goroutines. Events are routed to workers by hashing the event's `StreamID` with `fnv32a`, so all events of a given stream are processed sequentially on the same worker — preserving per-stream ordering while running different streams in parallel.

### Per-worker state: the factory

The handler factory you pass to the constructor is invoked once per worker, with a `WorkerContext` (`ID` in `[0, Count)`, `Count` = parallelism). Returning a fresh instance per call gives each worker private state, so anything the handler does in `Handle` (or `Commit`) is never shared across goroutines:

```go
relay := eventstore.NewPointerHandlerRelay(
    "high-throughput-relay",
    bundle.EventStore,
    bundle.IncrementIDStore,
    func(wc eventstore.WorkerContext) eventstore.Handler {
        // Per-worker instance: each worker gets its own connection.
        return &MyHandler{db: openPerWorkerDBConnection()}
    },
    eventstore.WithBatchSize(200),
    eventstore.WithParallelism(4),
)
```

If your factory returns the *same* instance to every worker, that instance is shared across all workers and you are responsible for making it safe for concurrent use. Return a fresh instance whenever your handler holds mutable state, a connection, or any resource that should not be shared.

### BatchHandler and the per-worker commit barrier

Handlers that need per-batch atomicity (e.g. an AMQP publisher that buffers messages and flushes them once per worker per batch) implement `BatchHandler` and are wired up with `NewPointerBatchHandlerRelay` / `NewTransientBatchHandlerRelay`:

```go
relay := eventstore.NewPointerBatchHandlerRelay(
    "amqp-relay",
    bundle.EventStore,
    bundle.IncrementIDStore,
    func(wc eventstore.WorkerContext) eventstore.BatchHandler {
        return &AMQPPublisher{channel: openPerWorkerAMQPChannel()}
    },
    eventstore.WithBatchSize(200),
    eventstore.WithParallelism(4),
)
```

```go
type AMQPPublisher struct{ /* ... */ }

func (p *AMQPPublisher) Name() string { return "amqp-publisher" }

func (p *AMQPPublisher) Handle(ctx context.Context, ev eventstore.StoredEvent) error {
    return p.enqueue(ev) // buffered into a per-worker channel
}

func (p *AMQPPublisher) Commit(ctx context.Context) error {
    return p.publishBatch(ctx) // atomic AMQP publish, runs once per worker per batch
}
```

`Commit(ctx)` is invoked once per worker after all of its events have been processed, before the relay advances the cursor (or, for `TransientRelay`, calls `CleanUpEvents`). This mirrors the PHP `MESSAGE_SYNC` / `MESSAGE_SYNC_ACK` barrier: per-worker work flushes atomically, then the next batch starts. If any handler returns an error, the pool cancels, no `Commit` runs, and the error propagates from `Run` so the surrounding retry loop can resume from the last committed position.

Plain `Handler` relays (`NewPointerHandlerRelay` / `NewTransientHandlerRelay`) have no `Commit` barrier — reach for a `BatchHandler` relay only when you need one. With `n == 1` (the default) a `BatchHandler` relay runs sequentially and `Commit` fires once after the last event of the batch.

## Glossary

The types below are the building blocks of the library. They combine into an append-only event log with cursor-based relays; for the transactional-outbox use case specifically, see the [transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html) and [polling publisher](https://microservices.io/patterns/data/polling-publisher.html) patterns.

### Events

**DomainEvent** — the write model you implement and pass to a store's append method. It is a [domain event](https://martinfowler.com/eaaDev/DomainEvent.html): it exposes the event's `ID`, `StreamID`, `EventType`, and `OccurredAt`; how the payload is serialized is up to the `Store` implementation. Optional cross-cutting metadata is exposed via `Metadata()` — see [Metadata](#metadata).

The event does **not** carry its position in the stream; per-stream versioning is enforced by `StreamStore.AppendWithExpectedVersion`, which takes the expected position as a separate argument. Aggregates that reload from history get the position from the last `StoredEvent.StreamVersion` they replayed.

**StoredEvent** — the read model delivered to handlers. It carries the database-assigned `IncrementID` (the relay's cursor position), the `StreamID` (used to partition events across parallel workers), the serialized `Payload`, the same `EventType` / `OccurredAt` metadata, the optional `Metadata` map, and the per-stream `StreamVersion` that aggregates use to reconstruct themselves.

#### Metadata

Domain events may carry cross-cutting `Metadata` as a `map[string]string`, attached via the `Metadata()` method. Use it for correlation IDs, causation chains, distributed-tracing IDs, tenancy, or any other key/value you want to propagate alongside an event.

Reserved keys (by convention — the store does not enforce them):

| Key | Purpose |
|---|---|
| `correlation_id` | links all events triggered by a single incoming request or command, propagated across service boundaries |
| `causation_id`   | the ID of the event that *caused* this event — i.e. the last event in the chain this one reacts to |
| `trace_id`       | OTel/distributed-tracing trace ID, for log correlation |

Custom keys are welcome. `nil` and an empty map are treated as equivalent and persisted as SQL `NULL` (no JSON overhead per row).

For events that have no metadata, embed `eventstore.BaseEvent` to satisfy the `Metadata()` method with `nil`:

```go
type OrderPlaced struct {
    eventstore.BaseEvent
    OrderID string
}
```

To attach metadata, override `Metadata()` on the embedding type:

```go
func (e OrderPlaced) Metadata() eventstore.Metadata {
    return eventstore.Metadata{
        eventstore.MetadataKeyCorrelationID: e.correlationID,
    }
}
```

#### Stream versioning

Streams are versioned per `(stream_id, stream_version)`. The version is *assigned by the store*, not carried on the event — this matches the [EventStoreDB `expectedVersion`](https://docs.kurrent.io/clients/tcp/dotnet/21.2/appending.md) and [Axon `expectedVersion`](https://docs.axoniq.io) pattern. Use `StreamStore.AppendWithExpectedVersion` to append events with an optimistic-concurrency check:

```go
err := store.AppendWithExpectedVersion(ctx, orderID, -1, evt)
//                                                    ↑
//                                "stream must currently be empty"
// On success the event is persisted at stream_version 0, 1, 2, … in batch order.
```

`expectedVersion == -1` means the stream must be empty (the create path); `expectedVersion == N` means the stream's current head must be exactly `N` (the case after replaying `N+1` events during Load).

The check and the insert run in the same transaction, so two writers loading the same stream cannot both append version `5` — the loser sees `ErrStreamVersionConflict` and reloads. The event's own payload never carries a version field, so there is no way for a caller to "forget" the version: it is the parameter that travels with the append.

A `DomainEvent` does not know its position. To reconstruct an aggregate, use `StreamReader.ReadStream(streamID, fromVersion, limit)` and track the last `StoredEvent.StreamVersion` you replayed — that is the `expectedVersion` you pass to the next `AppendWithExpectedVersion`. The inklusiv `fromVersion` semantics let you resume a rebuild from a snapshot's `version + 1` without an off-by-one dance.

For the plain outbox / projection use case (no aggregates, no per-stream ordering), use the simpler `Store.Append` path. It just inserts events in the order received and does not enforce per-stream ordering — the right tool for relays that just need durable, ordered event delivery.

### Stores

**Store** — the minimal write interface: `Append(ctx, ...DomainEvent)`. Every backend implements at least this. This is the plain append-only-log path: events are inserted in the order received and no per-stream ordering is enforced. The right tool for outbox / projection workloads that don't model aggregates.

**StreamStore** — write interface for per-stream appends with optimistic concurrency control: `AppendWithExpectedVersion(ctx, streamID, expectedVersion, ...DomainEvent)`. The store atomically verifies that the stream's current head equals `expectedVersion` (or `-1` for "stream must be empty") and assigns the new events consecutive stream versions starting at `expectedVersion + 1`. The right tool for the event-sourced aggregate pattern (Load → Decide → Save).

**PointerStore** — cursor-based reads: `FetchBatchOfEventsSince(lastIncrementID, limit)` returns events ordered by ascending `IncrementID`. Backs the pointer relays. The MySQL implementation applies gap detection so events are not delivered out of order while concurrent transactions are still in flight.

**TransientStore** — work-queue reads: `FetchBatchOfEvents(limit)` returns the head of the queue and `CleanUpEvents(events)` removes them after successful processing. Backs the transient relays, where each event is delivered once and then deleted.

**IncrementIDStore** — persists the last successfully processed `IncrementID` per relay (keyed by relay name), enabling resumption after restarts. `SetIncrementID` takes an expected previous value so implementations can enforce [optimistic concurrency control](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) (see `ErrIncrementIDConflict`).

**CleanUpToStore** — bulk outbox cleanup: `CleanUpToIncluding(incrementID)` removes every event at or below a position in one call. Useful when a relay has acknowledged a cursor and everything up to it can be discarded.

**StreamReader** — stream-scoped reads: `ReadStream(streamID, fromVersion, limit)` returns events for a single stream ordered by ascending `StreamVersion`, starting at `fromVersion` *inclusive* (so `fromVersion=0` returns the first event of the stream). Use it to reconstruct aggregates or to drive per-stream projections.

**StreamVersionReader** — cheap pre-check: `LatestStreamVersion(streamID)` returns the highest `StreamVersion` for a stream (or `-1` if the stream is empty) without loading any events. Use it to compute `expectedVersion` when you have not just loaded the stream (e.g. snapshot-resume, one-shot appenders), or for monitoring / diagnostics. Implementations are independent of `StreamReader` — declare a dependency on the one you actually need.

### Processing

**Relay** — fetches the next batch from a store and dispatches it to handlers. Create one with a constructor (`NewPointerHandlerRelay`, `NewTransientBatchHandlerRelay`, …) and call `Run` in a loop. See [Run a PointerRelay](#4-run-a-pointerrelay) and [Parallel relay](#parallel-relay-worker-pool).

**Handler** — processes a single `StoredEvent` via `Handle`, plus a `Name` for identification. Supplied to a relay through a factory (`func(WorkerContext) Handler`) so each worker can get its own instance.

**BatchHandler** — a `Handler` with an extra `Commit(ctx)` hook that fires once per worker after all of its events in a batch are handled, giving per-batch atomicity (e.g. buffer in `Handle`, flush in `Commit`). Used with the `BatchHandler` relays.

**WorkerContext** — passed to the factory to identify the worker (`ID` in `[0, Count)`, `Count` = parallelism), so factories can shard per-worker resources or tag logs/metrics.

### Error signals

**ErrEventNotReadyToProcess** — handlers return this to signal a temporary condition. The relay pauses (configurable via `WithConditionalBatchDelay`) instead of treating it as a hard failure.

**ErrIncrementIDConflict** — returned by an `IncrementIDStore` when the stored position changed between read and write, so a concurrent relay can detect it lost the optimistic-locking race.

**ErrStreamVersionConflict** — returned by `StreamStore.AppendWithExpectedVersion` when the stream's current head does not match `expectedVersion`. `errors.As` against `*eventstore.StreamVersionConflictError` exposes the conflicting `StreamID`, `EventID`, and the `Expected` vs. `Got` positions. Aggregates use this to reload and retry.

**ErrNilFactory** — returned from `Run` when a relay was constructed with a nil handler factory.

## Examples

Runnable examples live under [`integration/mysql/example`](integration/mysql/example):

```sh
# from integration/mysql
docker compose up --wait

go run ./example/append/              # Appending events with and without a transaction
go run ./example/outbox/              # Transactional outbox pattern (write + relay in one tx)
go run ./example/gap_detection/       # Two interleaved gaps: relay resumes in order, drops nothing
go run ./example/pointer_relay/       # PointerRelay with cursor-based position tracking
go run ./example/transient_relay/     # TransientRelay that deletes events after processing
go run ./example/repository_aggregate # Event-sourced aggregate: Load → Decide → Save, conflict retry
```

## Running integration tests

```bash
# Start MySQL (from integration/mysql)
docker compose up --wait

# Run
INTEGRATION_TESTS=1 go test ./integration/mysql/...
```

## License

MIT — see [LICENSE](LICENSE).

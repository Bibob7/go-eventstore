# Go Event Store

A lightweight Go library for the [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html). It provides the core abstractions for appending domain events and relaying them to handlers with gap-safe, cursor-based ordering (a [polling publisher](https://microservices.io/patterns/data/polling-publisher.html)).

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
    OutboxTableName:      "outbox",
    IncrementIDTableName: "event_increment_id",
}
```

## Quickstart

### 1. Implement DomainEvent

```go
type OrderPlaced struct {
    id          uuid.UUID
    aggregateID uuid.UUID
    occurredAt  time.Time
    OrderID     string
}

func (e OrderPlaced) ID() uuid.UUID          { return e.id }
func (e OrderPlaced) AggregateID() uuid.UUID { return e.aggregateID }
func (e OrderPlaced) EventType() string      { return "OrderPlaced" }
func (e OrderPlaced) OccurredAt() time.Time  { return e.occurredAt }
```

### 2. Append events

```go
store := mysqlstore.NewEventStore(db, "outbox")

err := store.Append(ctx, OrderPlaced{
    id:          uuid.Must(uuid.NewV4()),
    aggregateID: orderID,
    occurredAt:  time.Now(),
    OrderID:     "ord-123",
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
    OutboxTableName:      "outbox",
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
| `WithParallelism(n)` | Run handler calls across `n` worker goroutines partitioned by `EntityID` (default `1`). See [Parallel relay](#parallel-relay-worker-pool) below. |

## Parallel relay (worker pool)

For high-throughput outbox relays, `WithParallelism(n)` shards each batch across `n` worker goroutines. Events are routed to workers by hashing the event's `EntityID` with `fnv32a`, so all events of a given aggregate are processed sequentially on the same worker — preserving per-stream ordering while running different aggregates in parallel.

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

The types below are the building blocks of the library. For the broader patterns they implement, see the [transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html) and [polling publisher](https://microservices.io/patterns/data/polling-publisher.html) patterns.

### Events

**DomainEvent** — the write model you implement and pass to `Store.Append`. It is a [domain event](https://martinfowler.com/eaaDev/DomainEvent.html): it exposes the event's `ID`, `AggregateID`, `EventType`, and `OccurredAt`; how the payload is serialized is up to the `Store` implementation.

**StoredEvent** — the read model delivered to handlers. It carries the database-assigned `IncrementID` (the relay's cursor position), the `EntityID` (used to partition events across parallel workers), the serialized `Payload`, and the same `EventType` / `OccurredAt` metadata.

### Stores

**Store** — the minimal write interface: `Append(ctx, ...DomainEvent)`. Every backend implements at least this.

**PointerStore** — cursor-based reads: `FetchBatchOfEventsSince(lastIncrementID, limit)` returns events ordered by ascending `IncrementID`. Backs the pointer relays. The MySQL implementation applies gap detection so events are not delivered out of order while concurrent transactions are still in flight.

**TransientStore** — work-queue reads: `FetchBatchOfEvents(limit)` returns the head of the queue and `CleanUpEvents(events)` removes them after successful processing. Backs the transient relays, where each event is delivered once and then deleted.

**IncrementIDStore** — persists the last successfully processed `IncrementID` per relay (keyed by relay name), enabling resumption after restarts. `SetIncrementID` takes an expected previous value so implementations can enforce [optimistic concurrency control](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) (see `ErrIncrementIDConflict`).

**CleanUpToStore** — bulk outbox cleanup: `CleanUpToIncluding(incrementID)` removes every event at or below a position in one call. Useful when a relay has acknowledged a cursor and everything up to it can be discarded.

### Processing

**Relay** — fetches the next batch from a store and dispatches it to handlers. Create one with a constructor (`NewPointerHandlerRelay`, `NewTransientBatchHandlerRelay`, …) and call `Run` in a loop. See [Run a PointerRelay](#4-run-a-pointerrelay) and [Parallel relay](#parallel-relay-worker-pool).

**Handler** — processes a single `StoredEvent` via `Handle`, plus a `Name` for identification. Supplied to a relay through a factory (`func(WorkerContext) Handler`) so each worker can get its own instance.

**BatchHandler** — a `Handler` with an extra `Commit(ctx)` hook that fires once per worker after all of its events in a batch are handled, giving per-batch atomicity (e.g. buffer in `Handle`, flush in `Commit`). Used with the `BatchHandler` relays.

**WorkerContext** — passed to the factory to identify the worker (`ID` in `[0, Count)`, `Count` = parallelism), so factories can shard per-worker resources or tag logs/metrics.

### Error signals

**ErrEventNotReadyToProcess** — handlers return this to signal a temporary condition. The relay pauses (configurable via `WithConditionalBatchDelay`) instead of treating it as a hard failure.

**ErrIncrementIDConflict** — returned by an `IncrementIDStore` when the stored position changed between read and write, so a concurrent relay can detect it lost the optimistic-locking race.

**ErrNilFactory** — returned from `Run` when a relay was constructed with a nil handler factory.

## Examples

Runnable examples live under [`integration/mysql/example`](integration/mysql/example):

```sh
# from integration/mysql
docker compose up --wait

go run ./example/append/          # Appending events with and without a transaction
go run ./example/outbox/          # Transactional outbox pattern (write + relay in one tx)
go run ./example/pointer_relay/   # PointerRelay with cursor-based position tracking
go run ./example/transient_relay/ # TransientRelay that deletes events after processing
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

# Go Event Store

A lightweight Go library for the transactional outbox pattern. It provides the core abstractions for appending domain events and relaying them to consumers with gap-safe, cursor-based ordering.

## Modules

| Module | Description |
|--------|-------------|
| `github.com/Bibob7/go-eventstore` | Core interfaces, pointer relay, and transient relay (no DB dependency) |
| `github.com/Bibob7/go-eventstore/integration/mysql` | MySQL implementation of `Store`, `PointerStore`, `CleanUpStore`, and `IncrementIDStore` |

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

A `PointerRelay` tracks the last successfully processed `IncrementID` per consumer so it can resume after a restart without reprocessing events.

```go
bundle := mysqlstore.NewEventStoreBundle(db, mysqlstore.Config{
    OutboxTableName:      "outbox",
    IncrementIDTableName: "event_increment_id",
})

relay := eventstore.NewPointerRelay(
    "order-relay",
    bundle.EventStore,
    bundle.IncrementIDStore,
    eventstore.WithBatchSize(50),
    eventstore.WithConditionalBatchDelay(2*time.Second),
)
relay.RegisterHandler(&NotifyHandler{})

// run in a loop, e.g. with a ticker
for {
    if err := relay.Run(ctx); err != nil {
        log.Println("relay error:", err)
    }
    time.Sleep(time.Second)
}
```

> When using `WithParallelism(n)` with `n > 1`, register handlers via the
> factory methods (`RegisterHandlerFactory` / `RegisterBatchHandler`) so
> each worker gets its own private instance — see
> [Parallel relay](#parallel-relay-worker-pool). Plain `Handler`s passed to
> `RegisterHandler` are still shared across workers and must be safe for
> concurrent use. Handlers that implement `BatchHandler` get a per-worker
> `Commit` barrier; plain handlers are auto-wrapped with a no-op `Commit`.

### 5. Alternatively: TransientRelay

A `TransientRelay` deletes each event from the store after all handlers have processed it successfully. Useful when the outbox should not grow indefinitely and no separate cleanup job is desired.

```go
relay := eventstore.NewTransientRelay(
    "order-relay",
    bundle.EventStore, // EventStore also implements CleanUpStore
    eventstore.WithBatchSize(50),
)
relay.RegisterHandler(&NotifyHandler{})
```

## Relay options

| Option | Purpose |
|---|---|
| `WithBatchSize(n)` | Max events fetched per `Run` (default `DefaultBatchSize`). |
| `WithHandleDelay(d)` | Pause between individual events within a batch. |
| `WithBatchDelay(d)` | Unconditional delay between batches. |
| `WithConditionalBatchDelay(d)` | Delay applied only when a handler returns `ErrEventNotReadyToProcess`. |
| `WithParallelism(n)` | Run handler calls across `n` worker goroutines partitioned by `EntityID` (default `1`). See [Parallel relay](#parallel-relay-worker-pool) below. |

## Key concepts

**PointerStore** — fetches events since a given `IncrementID`. The MySQL implementation applies gap detection to avoid delivering events out of order while concurrent transactions are in-flight.

**IncrementIDStore** — persists the last successfully processed position per relay, enabling resumption after restarts. `SetIncrementID` uses an expected previous value so implementations can enforce optimistic locking.

**CleanUpStore** — used by `TransientRelay` to fetch and remove already-processed events.

**ErrEventNotReadyToProcess** — handlers return this to signal a temporary condition. The relay pauses (configurable via `WithConditionalBatchDelay`) instead of treating it as a hard failure.

## Parallel relay (worker pool)

For high-throughput outbox consumers, `WithParallelism(n)` shards each batch across `n` worker goroutines. Events are routed to workers by hashing the event's `EntityID` with `fnv32a`, so all events of a given aggregate are processed sequentially on the same worker — preserving per-stream ordering while running different aggregates in parallel.

### Per-worker state: use factories

When the same handler instance is shared across workers, anything the handler does in `Handle` (or `Commit`) is concurrently called by all workers. To avoid contention, register handlers via the factory methods so each worker gets its own private instance:

```go
relay := eventstore.NewPointerRelay(
    "high-throughput-relay",
    bundle.EventStore,
    bundle.IncrementIDStore,
    eventstore.WithBatchSize(200),
    eventstore.WithParallelism(4),
)

// Per-worker instance: the factory runs once per worker, so each worker
// gets its own *AMQPPublisher (e.g. with a private AMQP channel).
relay.RegisterHandlerFactory(func() eventstore.Handler {
    return &MyHandler{db: openPerWorkerDBConnection()}
})

relay.RegisterBatchHandler(func() eventstore.BatchHandler {
    return &AMQPPublisher{channel: openPerWorkerAMQPChannel()}
})
```

A plain handler passed via `RegisterHandler` is still shared across workers — the user is responsible for making it safe for concurrent use. Use the factory variants whenever your handler holds mutable state, a connection, or any resource that should not be shared.

### BatchHandler and the per-worker commit barrier

Handlers that need per-batch atomicity (e.g. an AMQP publisher that buffers messages and flushes them once per worker per batch) implement `BatchHandler`:

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

Plain `Handler`s (and `RegisterHandlerFactory` instances) are auto-wrapped with a no-op `Commit`, so existing handlers keep working unchanged. With `n == 1` (the default) the relay runs sequentially and behaves exactly as before, and `Commit` fires once after the last event of the batch.

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

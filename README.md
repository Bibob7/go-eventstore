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

## Key concepts

**PointerStore** — fetches events since a given `IncrementID`. The MySQL implementation applies gap detection to avoid delivering events out of order while concurrent transactions are in-flight.

**IncrementIDStore** — persists the last successfully processed position per relay, enabling resumption after restarts. `SetIncrementID` uses an expected previous value so implementations can enforce optimistic locking.

**CleanUpStore** — used by `TransientRelay` to fetch and remove already-processed events.

**ErrEventNotReadyToProcess** — handlers return this to signal a temporary condition. The relay pauses (configurable via `WithConditionalBatchDelay`) instead of treating it as a hard failure.

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

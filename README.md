# eventstore

A lightweight Go library for the transactional outbox pattern. It provides the core abstractions for appending domain events and relaying them to consumers — exactly once, with gap-safe ordering.

## Modules

| Module | Description |
|--------|-------------|
| `github.com/Bibob7/go-eventstore` | Core interfaces and relay logic (no DB dependency) |
| `github.com/Bibob7/go-eventstore/integration/mysql` | MySQL implementation of Store, IncrementIDStore, and IdempotencyRegistry |

## Installation

```bash
# Core module only
go get github.com/Bibob7/go-eventstore

# With MySQL integration
go get github.com/Bibob7/go-eventstore/integration/mysql
```

## Database setup

Apply the DDL from [`sql/mysql/schema.sql`](sql/mysql/schema.sql) to create the required tables. All table names are configurable via `eventstore.Config`.

```sql
-- example: use custom table names
OutboxTableName:      "my_outbox"
IncrementIDTableName: "my_relay_positions"
IdempotencyTableName: "my_idempotency"
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

func (e OrderPlaced) ID() uuid.UUID        { return e.id }
func (e OrderPlaced) AggregateID() uuid.UUID { return e.aggregateID }
func (e OrderPlaced) EventType() string    { return "OrderPlaced" }
func (e OrderPlaced) OccurredAt() time.Time { return e.occurredAt }
```

### 2. Append events

```go
store := mysql.NewEventStore(db, cfg.OutboxTableName)

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

### 4. Run a Relay

```go
bundle, _ := mysql.NewEventStoreBundle(db, cfg)

relay := eventstore.NewPointerRelay(
    "order-relay",
    bundle.EventStore,
    bundle.IncrementIDStore,
    eventstore.WithBatchSize(50),
    eventstore.WithBatchDelay(2*time.Second),
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

### 5. Idempotent processing (optional)

```go
registry := mysql.NewIdempotencyRegistry(db, cfg.IdempotencyTableName)

relay := eventstore.NewPointerRelay(
    "order-relay",
    bundle.EventStore,
    bundle.IncrementIDStore,
    eventstore.WithIdempotencyRegistry(registry),
)
```

## Key concepts

**PointerStore** — fetches events since a given `IncrementID`. The MySQL implementation applies gap detection to avoid delivering events out of order when concurrent transactions are in-flight.

**IncrementIDStore** — persists the last successfully processed position per relay, enabling resumption after restarts.

**IdempotencyRegistry** — prevents duplicate handler invocations. Uses a `pending → success | failed` state machine per event/handler pair.

**ErrEventNotReadyToProcess** — handlers can return this to signal a temporary condition. The relay will pause (configurable via `WithBatchDelay`) instead of treating it as a hard failure.

## Running integration tests

```bash
# Start MySQL
docker-compose -f docker-compose-test.yml up -d

# Run
INTEGRATION_TESTS=1 go test ./integration/mysql/...
```

## License

MIT — see [LICENSE](LICENSE).

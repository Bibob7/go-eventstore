# go-eventstore – MySQL Integration

MySQL implementation of the `go-eventstore` interfaces.

## Requirements

- Go 1.26+
- Docker (for running examples locally)

## Schema

The required tables are defined in [`sql/mysql/schema.sql`](sql/mysql/schema.sql):

| Table | Purpose |
|---|---|
| `outbox` | Stores domain events before they are relayed |
| `event_increment_id` | Persists the last processed position per relay consumer |

## Examples

Start a local MySQL instance:

```sh
docker compose up -d
```

Then run any example from this directory:

```sh
go run ./example/append/          # Appending events with and without a transaction
go run ./example/outbox/          # Transactional outbox pattern (write + relay in one tx)
go run ./example/pointer_relay/   # PointerRelay with cursor-based position tracking
go run ./example/transient_relay/ # TransientRelay that deletes events after processing
```

The default DSN matches the docker-compose setup. Override it via the `MYSQL_DSN` environment variable:

```sh
MYSQL_DSN="user:pass@tcp(host:3306)/db?parseTime=true" go run ./example/append/
```

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

Start a local MySQL instance and wait until it is healthy (MySQL needs ~30 s to
initialize on first boot):

```sh
docker compose up --wait
```

Then run any example from this directory:

```sh
go run ./example/append/          # Appending events with and without a transaction
go run ./example/outbox/          # Concurrent transactional outbox writes and gap detection
go run ./example/pointer_relay/   # PointerRelay with cursor-based position tracking
go run ./example/transient_relay/ # TransientRelay that deletes events after processing
```

The default DSN matches the docker-compose setup. Override it via the `MYSQL_DSN` environment variable:

```sh
MYSQL_DSN="user:pass@tcp(host:3306)/db" go run ./example/append/
```

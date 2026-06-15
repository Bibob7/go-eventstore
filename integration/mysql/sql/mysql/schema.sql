-- Event store table: stores domain events before they are relayed to handlers.
-- The table name is configurable via Config.EventStoreTableName.
--
-- stream_version is NULL for events written via the plain Store.Append path
-- (append-only log, no per-stream ordering) and a non-negative integer for
-- events written via StreamStore.AppendWithExpectedVersion. The UNIQUE key on
-- (stream_id, stream_version) is what makes the optimistic-concurrency check
-- safe under concurrent writers: two transactions that both pass the version
-- pre-check cannot both insert the same version — the loser fails with a
-- duplicate-key error that the store reports as ErrStreamVersionConflict.
-- MySQL permits any number of NULLs in a UNIQUE index, so the plain append
-- path is unaffected.
CREATE TABLE IF NOT EXISTS event_store (
    id            BIGINT         NOT NULL AUTO_INCREMENT,
    event_id      BINARY(16)     NOT NULL,
    stream_id     BINARY(16)     NOT NULL,
    stream_version INT           NULL,
    event_type    VARCHAR(255)   NOT NULL,
    payload       JSON           NOT NULL,
    occurred_at   DATETIME(6)    NOT NULL,
    metadata      JSON           NULL,
    PRIMARY KEY (id),
    KEY stream_id_idx  (stream_id),
    KEY event_type_idx (event_type),
    KEY event_id_idx   (event_id),
    UNIQUE KEY stream_version_uq (stream_id, stream_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Increment ID table: persists the last processed event ID per relay.
-- The table name is configurable via Config.IncrementIDTableName.
CREATE TABLE IF NOT EXISTS event_increment_id (
    relay_name   VARCHAR(255) NOT NULL,
    increment_id BIGINT       NOT NULL DEFAULT 0,
    PRIMARY KEY (relay_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
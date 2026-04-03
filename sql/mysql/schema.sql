-- Outbox table: stores domain events before they are relayed to consumers.
-- The table name is configurable via Config.OutboxTableName.
CREATE TABLE IF NOT EXISTS outbox (
    id            INT            NOT NULL AUTO_INCREMENT,
    event_id      BINARY(16)     NOT NULL,
    aggregate_id  BINARY(16)     NOT NULL,
    event_type    VARCHAR(255)   NOT NULL,
    payload       JSON           NOT NULL,
    occurred_at   DATETIME       NOT NULL,
    PRIMARY KEY (id),
    KEY aggregate_id_idx (aggregate_id),
    KEY event_type_idx   (event_type),
    KEY event_id_idx     (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Increment ID table: persists the last processed event ID per relay consumer.
-- The table name is configurable via Config.IncrementIDTableName.
CREATE TABLE IF NOT EXISTS event_increment_id (
    consumer_name VARCHAR(255) NOT NULL,
    increment_id  BIGINT       NOT NULL DEFAULT 0,
    PRIMARY KEY (consumer_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Idempotency registry: tracks event processing state to prevent duplicate handling.
-- The table name is configurable via Config.IdempotencyTableName.
CREATE TABLE IF NOT EXISTS idempotency_registry (
    idempotency_key BINARY(32)   NOT NULL,
    state           VARCHAR(16)  NOT NULL COMMENT 'pending | success | failed',
    created_at      DATETIME     NOT NULL,
    updated_at      DATETIME     NOT NULL,
    PRIMARY KEY (idempotency_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

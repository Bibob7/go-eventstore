-- Outbox table: stores domain events before they are relayed to handlers.
-- The table name is configurable via Config.EventStoreTableName.
CREATE TABLE IF NOT EXISTS outbox (
    id            INT            NOT NULL AUTO_INCREMENT,
    event_id      BINARY(16)     NOT NULL,
    stream_id     BINARY(16)     NOT NULL,
    event_type    VARCHAR(255)   NOT NULL,
    payload       JSON           NOT NULL,
    occurred_at   DATETIME       NOT NULL,
    PRIMARY KEY (id),
    KEY stream_id_idx  (stream_id),
    KEY event_type_idx (event_type),
    KEY event_id_idx   (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Increment ID table: persists the last processed event ID per relay.
-- The table name is configurable via Config.IncrementIDTableName.
CREATE TABLE IF NOT EXISTS event_increment_id (
    relay_name   VARCHAR(255) NOT NULL,
    increment_id BIGINT       NOT NULL DEFAULT 0,
    PRIMARY KEY (relay_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
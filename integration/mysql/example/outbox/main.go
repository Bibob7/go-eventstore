// outbox demonstrates the transactional outbox pattern:
// a business operation (placing an order) and its domain event are written
// to the database in a single transaction. A TransientRelay then picks up
// the event from the outbox and "publishes" it (here: prints it).
//
// Run with:
//
//	docker compose up --wait
//	go run ./example/outbox/
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"

	_ "github.com/go-sql-driver/mysql"

	eventstore "github.com/Bibob7/go-eventstore"
	mysqlstore "github.com/Bibob7/go-eventstore/integration/mysql"

	"github.com/Bibob7/go-eventstore/integration/mysql/example/shared"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	db, err := sql.Open("mysql", shared.DSN())
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("close db: %v", err)
		}
	}()

	if err := shared.WaitForDB(db); err != nil {
		return fmt.Errorf("db not ready: %w", err)
	}

	if err := createOrdersTable(db); err != nil {
		return fmt.Errorf("create orders table: %w", err)
	}

	bundle := mysqlstore.NewEventStoreBundle(db, mysqlstore.Config{
		OutboxTableName:      "outbox",
		IncrementIDTableName: "event_increment_id",
	})

	ctx := context.Background()

	// --- Step 1: Place an order atomically ---
	// Both the orders row and the domain event land in the DB in one transaction.
	// If either fails, neither is persisted – guaranteed by the database.
	fmt.Println("=== Step 1: Place order (transactional outbox write) ===")

	event := shared.NewOrderPlaced("alice", "keyboard", 1)

	err = mysqlstore.WithTransaction(ctx, db, func(tx *sql.Tx) error {
		// business state change
		_, err := tx.ExecContext(ctx,
			"INSERT INTO orders (id, customer_id, product, amount) VALUES (?, ?, ?, ?)",
			event.AggregateID().String(), event.CustomerID, event.Product, event.Amount,
		)
		if err != nil {
			return fmt.Errorf("insert order: %w", err)
		}

		// domain event – same transaction
		txCtx := mysqlstore.WithTx(ctx, tx)
		return bundle.EventStore.Append(txCtx, event)
	}, nil)
	if err != nil {
		return fmt.Errorf("place order: %w", err)
	}
	fmt.Printf("  order %s placed, event %s written to outbox\n", event.AggregateID(), event.ID())

	// --- Step 2: Relay picks up the event and "publishes" it ---
	fmt.Println("\n=== Step 2: TransientRelay processes outbox and cleans up ===")

	relay := eventstore.NewTransientRelay("outbox-publisher", bundle.EventStore,
		eventstore.WithBatchSize(50),
	)
	relay.RegisterHandler(&printHandler{})

	if err := relay.Run(ctx); err != nil {
		return fmt.Errorf("relay run: %w", err)
	}

	fmt.Println("\nOutbox is now empty – the event was consumed and deleted.")
	return nil
}

// printHandler prints every event it receives, simulating a message broker publish.
type printHandler struct{}

func (h *printHandler) Name() string { return "print-publisher" }

func (h *printHandler) Handle(_ context.Context, e eventstore.StoredEvent) error {
	var payload map[string]any
	_ = json.Unmarshal([]byte(e.Payload), &payload)
	fmt.Printf("  [published] type=%s id=%s payload=%v\n", e.EventType, e.ID, payload)
	return nil
}

func createOrdersTable(db *sql.DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS orders (
		id          VARCHAR(36)  NOT NULL,
		customer_id VARCHAR(255) NOT NULL,
		product     VARCHAR(255) NOT NULL,
		amount      INT          NOT NULL,
		created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	return err
}

// transient_relay demonstrates the TransientRelay, which deletes each event
// from the outbox immediately after successful processing. No position tracking
// is needed because consumed events are removed from the table.
//
// Run with:
//
//	docker compose up --wait
//	go run ./example/transient_relay/
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

	bundle, err := mysqlstore.NewEventStoreBundle(db, mysqlstore.Config{
		OutboxTableName:      "outbox",
		IncrementIDTableName: "event_increment_id",
	})
	if err != nil {
		return fmt.Errorf("create bundle: %w", err)
	}

	ctx := context.Background()

	// --- Step 1: Produce some events ---
	fmt.Println("=== Step 1: Appending events to the outbox ===")

	for _, order := range []struct {
		customer, product string
		amount            int
	}{
		{"alice", "keyboard", 1},
		{"bob", "monitor", 2},
		{"carol", "headphones", 3},
	} {
		e := shared.NewOrderPlaced(order.customer, order.product, order.amount)
		if err := bundle.EventStore.Append(ctx, e); err != nil {
			return fmt.Errorf("append: %w", err)
		}
		fmt.Printf("  appended %s (%s)\n", e.ID(), order.customer)
	}

	// --- Step 2: TransientRelay processes and removes events one by one ---
	fmt.Println("\n=== Step 2: TransientRelay processes outbox ===")

	relay := eventstore.NewTransientRelay(
		"transient-relay",
		bundle.EventStore,
		eventstore.WithBatchSize(10),
	)
	relay.RegisterHandler(&printHandler{})

	if err := relay.Run(ctx); err != nil {
		return fmt.Errorf("relay run: %w", err)
	}

	// --- Step 3: Run again – outbox is empty, nothing to process ---
	fmt.Println("\n=== Step 3: Re-running relay (outbox is now empty) ===")

	if err := relay.Run(ctx); err != nil {
		return fmt.Errorf("relay re-run: %w", err)
	}
	fmt.Println("  no events – outbox was empty")

	fmt.Println("\nDone.")
	return nil
}

// printHandler prints each event and simulates a downstream publish.
type printHandler struct{}

func (h *printHandler) Name() string { return "print-publisher" }

func (h *printHandler) Handle(_ context.Context, e eventstore.StoredEvent) error {
	var payload map[string]any
	_ = json.Unmarshal([]byte(e.Payload), &payload)
	fmt.Printf("  [processed & deleted] type=%s id=%s customer=%v\n",
		e.EventType, e.ID, payload["customer_id"])
	return nil
}

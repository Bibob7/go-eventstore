// append demonstrates how to append domain events to the outbox table,
// both directly and inside a database transaction.
//
// Run with:
//
//	docker compose up --wait
//	go run ./example/append/
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"os"

	_ "github.com/go-sql-driver/mysql"

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

	bundle := mysqlstore.NewEventStoreBundle(db, mysqlstore.Config{
		OutboxTableName:      "outbox",
		IncrementIDTableName: "event_increment_id",
	})

	ctx := context.Background()

	// --- 1. Append multiple events in a single call ---
	fmt.Println("=== Appending events without transaction ===")

	e1 := shared.NewOrderPlaced("alice", "keyboard", 1)
	e2 := shared.NewOrderPlaced("bob", "monitor", 2)

	if err := bundle.EventStore.Append(ctx, e1, e2); err != nil {
		return fmt.Errorf("append: %w", err)
	}
	fmt.Printf("  appended %s (alice / keyboard)\n", e1.ID())
	fmt.Printf("  appended %s (bob / monitor)\n", e2.ID())

	// --- 2. Append inside a transaction ---
	// In a real service you would also INSERT your business entity here
	// so the event and the state change commit atomically.
	fmt.Println("\n=== Appending event inside a transaction ===")

	err = mysqlstore.WithTransaction(ctx, db, func(tx *sql.Tx) error {
		txCtx := mysqlstore.WithTx(ctx, tx)

		e3 := shared.NewOrderPlaced("carol", "headphones", 3)
		if err := bundle.EventStore.Append(txCtx, e3); err != nil {
			return fmt.Errorf("append in tx: %w", err)
		}
		fmt.Printf("  appended %s (carol / headphones) – commits atomically with tx\n", e3.ID())
		return nil
	}, nil)
	if err != nil {
		return fmt.Errorf("transaction: %w", err)
	}

	fmt.Println("\nDone. Inspect the outbox table to see the stored events.")
	return nil
}

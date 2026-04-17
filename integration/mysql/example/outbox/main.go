// outbox demonstrates why gap detection is needed for a transactional outbox:
// two concurrent repository writes append events in separate transactions, but
// the slower transaction keeps the smaller auto-increment ID uncommitted long
// enough for the faster transaction to commit behind it.
//
// Run with:
//
//	docker compose up --wait
//	go run ./example/outbox/
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/Bibob7/go-eventstore"
	"github.com/Bibob7/go-eventstore/integration/mysql"

	"github.com/Bibob7/go-eventstore/integration/mysql/example/shared"
)

const (
	slowTransactionDelay = 2 * time.Second
	fastTransactionDelay = 200 * time.Millisecond
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

	if err := prepareDemoTables(db); err != nil {
		return fmt.Errorf("prepare demo tables: %w", err)
	}

	store := mysql.NewEventStore(db, "outbox")
	repo := &orderRepository{db: db, eventStore: store}

	ctx := context.Background()
	slowInserted := make(chan struct{})
	fastInserted := make(chan struct{})
	fastCommitted := make(chan struct{})

	slowOrder := newOrder(shared.NewOrderPlaced("alice", "keyboard", 1))
	fastOrder := newOrder(shared.NewOrderPlaced("bob", "monitor", 2))

	var (
		wg   sync.WaitGroup
		errs = make(chan error, 2)
	)

	fmt.Println("=== Step 1: Run two transactional repository writes concurrently ===")

	wg.Add(2)
	go func() {
		defer wg.Done()
		fmt.Printf("  [slow] start order=%s hold=%s\n", slowOrder.ID, slowTransactionDelay)
		errs <- repo.Persist(ctx, slowOrder, slowTransactionDelay, func() {
			fmt.Println("  [slow] business row + outbox event inserted, transaction still open")
			close(slowInserted)
		})
	}()

	<-slowInserted

	go func() {
		defer wg.Done()
		fmt.Printf("  [fast] start order=%s hold=%s\n", fastOrder.ID, fastTransactionDelay)
		errs <- repo.Persist(ctx, fastOrder, fastTransactionDelay, func() {
			fmt.Println("  [fast] business row + outbox event inserted, transaction still open")
			close(fastInserted)
		})
		close(fastCommitted)
	}()

	<-fastInserted
	<-fastCommitted

	fmt.Println("\n=== Step 2: Fetch while the lower increment ID is still uncommitted ===")
	blockedEvents, err := store.FetchBatchOfEventsSince(ctx, 0, 10)
	if err != nil {
		return fmt.Errorf("fetch before commit: %w", err)
	}
	printEvents("before slow commit", blockedEvents)

	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}

	fmt.Println("\n=== Step 3: Fetch again after both transactions committed ===")
	visibleEvents, err := store.FetchBatchOfEventsSince(ctx, 0, 10)
	if err != nil {
		return fmt.Errorf("fetch after commit: %w", err)
	}
	printEvents("after both commits", visibleEvents)

	fmt.Println("\nDone. The first fetch stops at the gap; the second sees both events in order.")
	return nil
}

func printEvents(label string, events []eventstore.StoredEvent) {
	fmt.Printf("  [%s] fetched %d event(s)\n", label, len(events))
	for _, event := range events {
		fmt.Printf("    increment_id=%d type=%s aggregate_id=%s\n",
			event.IncrementID, event.EventType, event.EntityID)
	}
}

// pointer_relay demonstrates the PointerRelay, which tracks the last processed
// event position (IncrementID) so it can resume after a restart without
// reprocessing events. Multiple independent relays (consumers) can subscribe
// to the same outbox with their own cursors.
//
// Run with:
//
//	docker compose up --wait
//	go run ./example/pointer_relay/
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync"
	"time"

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

	for _, e := range []*shared.OrderPlaced{
		shared.NewOrderPlaced("alice", "keyboard", 1),
		shared.NewOrderPlaced("bob", "monitor", 2),
		shared.NewOrderPlaced("carol", "headphones", 3),
	} {
		if err := bundle.EventStore.Append(ctx, e); err != nil {
			return fmt.Errorf("append: %w", err)
		}
		fmt.Printf("  appended %s (%s)\n", e.ID(), e.CustomerID)
	}

	// --- Step 2: Two independent consumers, each with their own cursor ---
	fmt.Println("\n=== Step 2: Running two PointerRelays concurrently ===")

	analyticsRelay := eventstore.NewPointerRelay(
		"analytics-consumer",
		bundle.EventStore,
		bundle.IncrementIDStore,
		eventstore.WithBatchSize(10),
	)
	analyticsRelay.RegisterHandler(&loggingHandler{name: "analytics"})

	notificationRelay := eventstore.NewPointerRelay(
		"notification-consumer",
		bundle.EventStore,
		bundle.IncrementIDStore,
		eventstore.WithBatchSize(10),
	)
	notificationRelay.RegisterHandler(&loggingHandler{name: "notifications"})

	if err := runConcurrent(ctx, analyticsRelay, notificationRelay); err != nil {
		return err
	}

	// --- Step 3: New events arrive; relays resume from their saved cursor ---
	fmt.Println("\n=== Step 3: Appending one more event ===")

	newEvent := shared.NewOrderPlaced("dave", "webcam", 4)
	if err := bundle.EventStore.Append(ctx, newEvent); err != nil {
		return fmt.Errorf("append: %w", err)
	}
	fmt.Printf("  appended %s (dave)\n", newEvent.ID())

	fmt.Println("\n=== Step 4: Re-running relays – only the new event is processed ===")
	if err := runConcurrent(ctx, analyticsRelay, notificationRelay); err != nil {
		return err
	}

	fmt.Println("\nDone. Events remain in the outbox (PointerRelay only tracks position).")
	return nil
}

func runConcurrent(ctx context.Context, relays ...eventstore.Relay) error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for _, r := range relays {
		wg.Add(1)
		go func(relay eventstore.Relay) {
			defer wg.Done()
			if err := relay.Run(ctx); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("relay %s: %w", relay.Name(), err))
				mu.Unlock()
			}
		}(r)
	}
	wg.Wait()
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// loggingHandler prints each event it processes.
type loggingHandler struct {
	name string
	mu   sync.Mutex
	n    int
}

func (h *loggingHandler) Name() string { return h.name }

func (h *loggingHandler) Handle(_ context.Context, e eventstore.StoredEvent) error {
	h.mu.Lock()
	h.n++
	n := h.n
	h.mu.Unlock()

	var payload map[string]any
	_ = json.Unmarshal([]byte(e.Payload), &payload)
	fmt.Printf("  [%s] #%d  type=%s customer=%v\n", h.name, n, e.EventType, payload["customer_id"])
	time.Sleep(10 * time.Millisecond)
	return nil
}

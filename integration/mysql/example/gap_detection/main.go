// gap_detection demonstrates that a PointerRelay never loses or reorders events
// even when slower transactions leave lower auto-increment IDs uncommitted
// behind already-committed higher IDs.
//
// This is the harder case the simple outbox example only hints at: instead of a
// single gap and a raw fetch, two slow writers each hold a transaction open at
// the same time, producing *two* separate gaps in the increment-ID sequence.
// A PointerRelay then polls repeatedly. On every poll the gap filter stops at
// the first still-open gap, so the cursor never jumps past an ID that might
// still commit. As each slow transaction commits, a later poll resumes exactly
// where it left off.
//
// The increment-ID timeline this example builds:
//
//	id=1  slow-A   inserted, transaction held open      (gap #1)
//	id=2  fast-1   committed immediately
//	id=3  slow-B   inserted, transaction held open      (gap #2)
//	id=4  fast-2   committed immediately
//
// Poll 1 (both gaps open):   processes nothing – stops before id=1.
// commit slow-A (id=1)
// Poll 2 (gap #2 still open): processes 1,2 – stops before id=3.
// commit slow-B (id=3)
// Poll 3 (no gaps left):      processes 3,4.
//
// At the end the example asserts every event was processed exactly once and in
// strict increment-ID order. Without gap detection, poll 1 would have consumed
// id=2 and id=4 and parked the cursor at 4, silently dropping id=1 and id=3
// forever.
//
// Run with:
//
//	docker compose up --wait
//	go run ./example/gap_detection/
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"reflect"

	_ "github.com/go-sql-driver/mysql"

	"github.com/Bibob7/go-eventstore"
	"github.com/Bibob7/go-eventstore/integration/mysql"

	"github.com/Bibob7/go-eventstore/integration/mysql/example/shared"
)

const relayName = "gap-detection-relay"

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

	bundle := mysql.NewEventStoreBundle(db, mysql.Config{
		EventStoreTableName:  "event_store",
		IncrementIDTableName: "event_increment_id",
	})
	repo := &orderRepository{db: db, eventStore: bundle.EventStore}

	ctx := context.Background()

	// collector records the order in which the relay hands events to the
	// handler, so we can prove nothing was skipped or reordered at the end.
	collector := &collectingHandler{}
	relay := eventstore.NewPointerHandlerRelay(
		relayName,
		bundle.EventStore,
		bundle.IncrementIDStore,
		func(eventstore.WorkerContext) eventstore.Handler { return collector },
		eventstore.WithBatchSize(100),
	)

	// --- Step 1: Build two interleaved gaps ---
	fmt.Println("=== Step 1: Two slow writers hold transactions open, two fast writers commit ===")

	slowA := repo.persistHolding(ctx, newOrder(shared.NewOrderPlaced("alice", "keyboard", 1)))
	fmt.Println("  [slow-A] inserted (id=1), transaction held open  -> gap #1")

	if err := repo.persistNow(ctx, newOrder(shared.NewOrderPlaced("bob", "monitor", 2))); err != nil {
		return fmt.Errorf("fast-1 persist: %w", err)
	}
	fmt.Println("  [fast-1] committed (id=2)")

	slowB := repo.persistHolding(ctx, newOrder(shared.NewOrderPlaced("carol", "headphones", 3)))
	fmt.Println("  [slow-B] inserted (id=3), transaction held open  -> gap #2")

	if err := repo.persistNow(ctx, newOrder(shared.NewOrderPlaced("dave", "webcam", 4))); err != nil {
		return fmt.Errorf("fast-2 persist: %w", err)
	}
	fmt.Println("  [fast-2] committed (id=4)")

	// --- Step 2: Poll while both gaps are open ---
	fmt.Println("\n=== Step 2: Poll relay – both gaps still open ===")
	if err := relay.Run(ctx); err != nil {
		return fmt.Errorf("poll 1: %w", err)
	}
	if err := reportCursor(ctx, bundle.IncrementIDStore, collector, "after poll 1"); err != nil {
		return err
	}

	// --- Step 3: Close gap #1, poll again ---
	fmt.Println("\n=== Step 3: Commit slow-A, then poll – gap #2 still open ===")
	if err := slowA.commit(); err != nil {
		return fmt.Errorf("commit slow-A: %w", err)
	}
	fmt.Println("  [slow-A] committed (id=1)")
	if err := relay.Run(ctx); err != nil {
		return fmt.Errorf("poll 2: %w", err)
	}
	if err := reportCursor(ctx, bundle.IncrementIDStore, collector, "after poll 2"); err != nil {
		return err
	}

	// --- Step 4: Close gap #2, poll again ---
	fmt.Println("\n=== Step 4: Commit slow-B, then poll – no gaps left ===")
	if err := slowB.commit(); err != nil {
		return fmt.Errorf("commit slow-B: %w", err)
	}
	fmt.Println("  [slow-B] committed (id=3)")
	if err := relay.Run(ctx); err != nil {
		return fmt.Errorf("poll 3: %w", err)
	}
	if err := reportCursor(ctx, bundle.IncrementIDStore, collector, "after poll 3"); err != nil {
		return err
	}

	// --- Step 5: Verify correctness ---
	fmt.Println("\n=== Step 5: Verify nothing was skipped or reordered ===")
	want := []int64{1, 2, 3, 4}
	got := collector.ProcessedIDs()
	if !reflect.DeepEqual(want, got) {
		return fmt.Errorf("gap detection failed: processed %v, want %v", got, want)
	}
	fmt.Printf("  processed order = %v (every event exactly once, in order)\n", got)

	fmt.Println("\nDone. Gap detection held the cursor at each open gap and resumed in order once it closed.")
	return nil
}

func reportCursor(ctx context.Context, store eventstore.IncrementIDStore, c *collectingHandler, label string) error {
	cursor, err := store.GetIncrementID(ctx, relayName)
	if err != nil {
		return fmt.Errorf("get cursor: %w", err)
	}
	fmt.Printf("  [%s] cursor=%d processed so far=%v\n", label, cursor, c.ProcessedIDs())
	return nil
}

// collectingHandler records every event the relay dispatches, in order.
type collectingHandler struct {
	ids []int64
}

func (h *collectingHandler) Name() string { return "collector" }

func (h *collectingHandler) Handle(_ context.Context, e eventstore.StoredEvent) error {
	h.ids = append(h.ids, e.IncrementID)
	var payload map[string]any
	_ = json.Unmarshal([]byte(e.Payload), &payload)
	fmt.Printf("    [processed] increment_id=%d type=%s customer=%v\n", e.IncrementID, e.EventType, payload["customer_id"])
	return nil
}

func (h *collectingHandler) ProcessedIDs() []int64 {
	out := make([]int64, len(h.ids))
	copy(out, h.ids)
	return out
}

func prepareDemoTables(db *sql.DB) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS orders (
			id          VARCHAR(36)  NOT NULL,
			customer_id VARCHAR(255) NOT NULL,
			product     VARCHAR(255) NOT NULL,
			amount      INT          NOT NULL,
			created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		"TRUNCATE TABLE orders",
		"TRUNCATE TABLE event_store",
		"TRUNCATE TABLE event_increment_id",
	}
	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

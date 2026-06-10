// repository_aggregate demonstrates the event-sourced aggregate pattern on top
// of the event store: read a stream to rebuild the aggregate's state, run
// domain logic that decides which new events to emit, and append them with
// the right expectedVersion. The whole load/decide/save cycle runs inside one
// MySQL transaction so concurrent aggregates can't trample each other.
//
// The example shows two ways to source the expectedVersion for the append:
//   - Hot path: the aggregate's in-memory Version (set during LoadOrder).
//   - Source path: StreamVersionReader.LatestStreamVersion, useful when you
//     have not just loaded the aggregate (here: the CreateOrder path,
//     which expects an empty stream and uses expectedVersion = -1).
//
// Run with:
//
//	docker compose up --wait
//	go run ./example/repository_aggregate/
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gofrs/uuid/v5"

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

	// *EventStore satisfies eventstore.Store, eventstore.StreamStore,
	// eventstore.StreamReader, and eventstore.StreamVersionReader — one
	// concrete type, four roles. The repository picks the role it needs.
	store := mysqlstore.NewEventStore(db, "event_store")

	repo := &orderRepository{
		db:     db,
		store:  store,
		stream: store,
		heads:  store,
	}
	ctx := context.Background()

	// --- 1. First time we see this order: create the fresh stream. ---
	// CreateOrder uses LatestStreamVersion (Source path) to discover
	// that the stream is empty — no LoadOrder needed.
	orderID := uuid.Must(uuid.NewV4())
	fmt.Printf("=== Step 1: create order %s (fresh stream, source path) ===\n", orderID)
	if err := repo.CreateOrder(ctx, orderID, "alice", "keyboard", 1); err != nil {
		return fmt.Errorf("create order: %w", err)
	}

	// --- 2. Load the aggregate from history and add a second item. ---
	// (We name the local variable `aggregate` to keep the type name `order`
	// — the struct defined below — unshadowed.)
	fmt.Println("\n=== Step 2: load order from history, add a second item (hot path) ===")
	aggregate, err := repo.LoadOrder(ctx, orderID)
	if err != nil {
		return fmt.Errorf("load order: %w", err)
	}
	fmt.Printf("  loaded order with %d item(s) at version %d\n", len(aggregate.Items), aggregate.Version)
	if err := repo.AddItem(ctx, aggregate, "mouse", 1); err != nil {
		return fmt.Errorf("add item: %w", err)
	}

	// --- 3. Read the stream back to see the resulting event log. ---
	fmt.Println("\n=== Step 3: replay the stream to verify the log ===")
	events, err := store.ReadStream(ctx, orderID, 0, 100)
	if err != nil {
		return fmt.Errorf("read stream: %w", err)
	}
	for _, e := range events {
		fmt.Printf("  v=%d  type=%s\n", e.StreamVersion, e.EventType)
	}

	// --- 4. Simulate a stale write: try to add an item using an old version. ---
	// staleAggregate is a *different* pointer than `aggregate` above but
	// carries a version that is out of date — a realistic proxy for a
	// cached aggregate that wasn't refreshed before re-saving.
	fmt.Println("\n=== Step 4: simulate a concurrent writer with a stale version ===")
	staleAggregate := &order{
		ID:      orderID,
		Version: 0, // last version we observed (stale — the real last version is higher)
		Items:   []item{{Name: "stale", Qty: 1}},
	}
	err = repo.AddItem(ctx, staleAggregate, "from-stale-cache", 1)
	if err == nil {
		return errors.New("expected a version conflict, got nil")
	}
	var conflict *eventstore.StreamVersionConflictError
	if !errors.As(err, &conflict) {
		return fmt.Errorf("expected *StreamVersionConflictError, got %T: %w", err, err)
	}
	fmt.Printf("  conflict detected: stream=%s expected=%d got=%d — caller can reload and retry\n",
		conflict.StreamID, conflict.Expected, conflict.Got)

	fmt.Println("\nDone. The aggregate pattern in 3 steps: Load → Decide → Save, all in one TX.")
	return nil
}

// --- Domain -------------------------------------------------------------

// item is the projection the order aggregate keeps in memory.
type item struct {
	Name string
	Qty  int
}

// order is the in-memory projection of an order aggregate, rebuilt from
// events by replaying its stream. The Version tracks the highest
// StreamVersion the aggregate has observed and is what AddItem passes
// back to StreamStore.AppendWithExpectedVersion as the expectedVersion.
type order struct {
	ID      uuid.UUID
	Version int
	Items   []item
}

func (o *order) addItem(name string, qty int) error {
	if qty <= 0 {
		return errors.New("qty must be > 0")
	}
	o.Items = append(o.Items, item{Name: name, Qty: qty})
	return nil
}

// --- Events -------------------------------------------------------------

// orderCreated and itemAdded are domain events. They embed eventstore.BaseEvent
// for the ID/StreamID/EventType/OccurredAt plumbing; the application code
// overrides Metadata when it needs to. Note that the events do NOT carry a
// StreamVersion — the per-stream position is assigned by the store on append
// and tracked on the aggregate via the last StoredEvent.StreamVersion seen
// during Load.
type orderCreated struct {
	eventstore.BaseEvent
	EventID    uuid.UUID `json:"event_id"`
	OrderID    uuid.UUID `json:"order_id"`
	CustomerID string    `json:"customer_id"`
	OccurredOn time.Time `json:"occurred_at"`
	Product    string    `json:"product"`
	Qty        int       `json:"qty"`
}

func (e *orderCreated) ID() uuid.UUID         { return e.EventID }
func (e *orderCreated) StreamID() uuid.UUID   { return e.OrderID }
func (e *orderCreated) EventType() string     { return "order.created" }
func (e *orderCreated) OccurredAt() time.Time { return e.OccurredOn }

type itemAdded struct {
	eventstore.BaseEvent
	EventID    uuid.UUID `json:"event_id"`
	OrderID    uuid.UUID `json:"order_id"`
	OccurredOn time.Time `json:"occurred_at"`
	Product    string    `json:"product"`
	Qty        int       `json:"qty"`
}

func (e *itemAdded) ID() uuid.UUID         { return e.EventID }
func (e *itemAdded) StreamID() uuid.UUID   { return e.OrderID }
func (e *itemAdded) EventType() string     { return "order.item_added" }
func (e *itemAdded) OccurredAt() time.Time { return e.OccurredOn }

// --- Repository ---------------------------------------------------------

// orderRepository wires the event store to the order aggregate. It depends
// on three of the four roles the *EventStore plays:
//   - eventstore.StreamStore   (write with optimistic concurrency)
//   - eventstore.StreamReader  (read a single stream to rebuild state)
//   - eventstore.StreamVersionReader (cheap "is the stream empty?" probe)
//
// The interfaces are satisfied by the same *mysqlstore.EventStore, so the
// caller only has to pass one value into the constructor.
type orderRepository struct {
	db     *sql.DB
	store  eventstore.StreamStore
	stream eventstore.StreamReader
	heads  eventstore.StreamVersionReader
}

// CreateOrder appends the first event of a fresh stream. The expectedVersion
// here is -1 ("stream must be empty") and we use LatestStreamVersion to
// verify the stream really is empty — the *Source path*, where the caller
// has not loaded the aggregate and learns its position from a probe
// instead. The expectedVersion check inside the transaction is what
// prevents two concurrent CreateOrder calls from both succeeding.
func (r *orderRepository) CreateOrder(ctx context.Context, orderID uuid.UUID, customer, product string, qty int) error {
	return mysqlstore.WithTransaction(ctx, r.db, func(tx *sql.Tx) error {
		txCtx := mysqlstore.WithTx(ctx, tx)

		// Source path: read the current head via StreamVersionReader
		// (cheap single-aggregate query) instead of replaying the
		// stream. For a fresh order this returns -1.
		head, err := r.heads.LatestStreamVersion(txCtx, orderID)
		if err != nil {
			return err
		}
		if head != -1 {
			return fmt.Errorf("order %s already exists at version %d", orderID, head)
		}

		evt := &orderCreated{
			EventID:    uuid.Must(uuid.NewV4()),
			OrderID:    orderID,
			CustomerID: customer,
			OccurredOn: time.Now().UTC(),
			Product:    product,
			Qty:        qty,
		}
		// expectedVersion = -1 means "stream must currently be empty".
		// The store verifies this atomically and assigns the event
		// stream_version = 0.
		return r.store.AppendWithExpectedVersion(txCtx, orderID, -1, evt)
	}, nil)
}

// LoadOrder rebuilds the aggregate by replaying its stream. This is the
// "Load" step of the Load/Decide/Save pattern. The last
// StoredEvent.StreamVersion the aggregate sees becomes the in-memory
// Version, which is the expectedVersion the next AddItem will pass back.
func (r *orderRepository) LoadOrder(ctx context.Context, orderID uuid.UUID) (*order, error) {
	// ReadStream uses inklusiv fromVersion semantics, so fromVersion=0 starts
	// at the first event. There's no upper limit on the read — for very
	// long-lived streams you'd want snapshot support and resume from
	// snapshotVersion+1, but that's out of scope for this example.
	const noLimit = 1_000_000
	history, err := r.stream.ReadStream(ctx, orderID, 0, noLimit)
	if err != nil {
		return nil, err
	}
	if len(history) == 0 {
		return nil, fmt.Errorf("order %s not found", orderID)
	}

	o := &order{ID: orderID}
	for _, e := range history {
		o.Version = e.StreamVersion
		var payload struct {
			Product string `json:"product"`
			Qty     int    `json:"qty"`
		}
		if err := json.Unmarshal([]byte(e.Payload), &payload); err != nil {
			return nil, fmt.Errorf("decode event %s: %w", e.ID, err)
		}
		switch e.EventType {
		case "order.created":
			o.Items = []item{{Name: payload.Product, Qty: payload.Qty}}
		case "order.item_added":
			o.Items = append(o.Items, item{Name: payload.Product, Qty: payload.Qty})
		}
	}
	return o, nil
}

// AddItem runs Decide + Save in a single transaction. The aggregate's
// in-memory Version — set during LoadOrder — is passed back to
// AppendWithExpectedVersion as the expectedVersion. If a concurrent
// aggregate already appended to this stream between our Load and now,
// the store's check raises ErrStreamVersionConflict and the transaction
// rolls back. This is the *Hot path* of the optimistic-concurrency
// contract: no extra read on the happy path.
func (r *orderRepository) AddItem(ctx context.Context, o *order, product string, qty int) error {
	return mysqlstore.WithTransaction(ctx, r.db, func(tx *sql.Tx) error {
		txCtx := mysqlstore.WithTx(ctx, tx)

		// Decide: ask the aggregate to validate the new state.
		if err := o.addItem(product, qty); err != nil {
			return err
		}

		// Save: emit the new event. The expectedVersion is whatever the
		// aggregate last observed on Load — no extra probe needed.
		evt := &itemAdded{
			EventID:    uuid.Must(uuid.NewV4()),
			OrderID:    o.ID,
			OccurredOn: time.Now().UTC(),
			Product:    product,
			Qty:        qty,
		}
		return r.store.AppendWithExpectedVersion(txCtx, o.ID, o.Version, evt)
	}, nil)
}

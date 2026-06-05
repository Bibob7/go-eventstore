package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Bibob7/go-eventstore"
	"github.com/Bibob7/go-eventstore/integration/mysql"

	"github.com/Bibob7/go-eventstore/integration/mysql/example/shared"
)

type order struct {
	ID       string
	Customer string
	Product  string
	Amount   int
	Events   []eventstore.DomainEvent
}

func newOrder(event *shared.OrderPlaced) order {
	return order{
		ID:       event.StreamID().String(),
		Customer: event.CustomerID,
		Product:  event.Product,
		Amount:   event.Amount,
		Events:   []eventstore.DomainEvent{event},
	}
}

type orderRepository struct {
	db         *sql.DB
	eventStore *mysql.EventStore
}

// persistNow writes the order and its outbox event in a single transaction and
// commits immediately, so the event's auto-increment ID becomes visible at once.
func (r *orderRepository) persistNow(ctx context.Context, o order) error {
	return mysql.WithTransaction(ctx, r.db, func(tx *sql.Tx) error {
		return insertOrderAndEvent(ctx, tx, r.eventStore, o)
	}, nil)
}

// heldWrite is an in-flight transaction whose INSERTs (and therefore
// auto-increment IDs) have already happened, but which is held open — and thus
// invisible to committed-read fetches — until commit is called.
type heldWrite struct {
	release chan struct{}
	done    chan error
}

// commit releases the held transaction and waits for it to finish committing.
func (h *heldWrite) commit() error {
	close(h.release)
	return <-h.done
}

// persistHolding starts the transaction, performs the INSERTs (claiming the next
// auto-increment ID), and then blocks the transaction open until commit is
// called. It returns only once the INSERTs are durable within the transaction,
// so the caller can rely on the ID ordering of subsequent writes.
func (r *orderRepository) persistHolding(ctx context.Context, o order) *heldWrite {
	h := &heldWrite{
		release: make(chan struct{}),
		done:    make(chan error, 1),
	}
	inserted := make(chan struct{})
	go func() {
		h.done <- mysql.WithTransaction(ctx, r.db, func(tx *sql.Tx) error {
			if err := insertOrderAndEvent(ctx, tx, r.eventStore, o); err != nil {
				return err
			}
			close(inserted)
			<-h.release // hold the transaction open until released
			return nil
		}, nil)
	}()
	<-inserted
	return h
}

func insertOrderAndEvent(ctx context.Context, tx *sql.Tx, store *mysql.EventStore, o order) error {
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO orders (id, customer_id, product, amount) VALUES (?, ?, ?, ?)",
		o.ID, o.Customer, o.Product, o.Amount,
	); err != nil {
		return fmt.Errorf("insert order: %w", err)
	}
	if err := store.Append(mysql.WithTx(ctx, tx), o.Events...); err != nil {
		return fmt.Errorf("append outbox event: %w", err)
	}
	return nil
}

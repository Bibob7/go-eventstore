package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

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
		ID:       event.AggregateID().String(),
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

func (r *orderRepository) Persist(ctx context.Context, order order, holdTxFor time.Duration, afterInsert func()) error {
	return mysql.WithTransaction(ctx, r.db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO orders (id, customer_id, product, amount) VALUES (?, ?, ?, ?)",
			order.ID, order.Customer, order.Product, order.Amount,
		); err != nil {
			return fmt.Errorf("insert order: %w", err)
		}

		txCtx := mysql.WithTx(ctx, tx)
		if err := r.eventStore.Append(txCtx, order.Events...); err != nil {
			return fmt.Errorf("append outbox events: %w", err)
		}

		if afterInsert != nil {
			afterInsert()
		}
		time.Sleep(holdTxFor)
		fmt.Printf("  [repo] commit order=%s after %s\n", order.ID, holdTxFor)
		return nil
	}, nil)
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
		"TRUNCATE TABLE outbox",
	}
	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

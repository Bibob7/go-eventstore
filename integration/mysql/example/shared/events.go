package shared

import (
	"time"

	"github.com/gofrs/uuid/v5"
)

// OrderPlaced is a domain event used across all examples.
type OrderPlaced struct {
	EventID    uuid.UUID `json:"event_id"`
	OrderID    uuid.UUID `json:"order_id"`
	CustomerID string    `json:"customer_id"`
	Product    string    `json:"product"`
	Amount     int       `json:"amount"`
	OccurredOn time.Time `json:"occurred_at"`
}

func NewOrderPlaced(customerID, product string, amount int) *OrderPlaced {
	id, err := uuid.NewV4()
	if err != nil {
		panic("shared.NewOrderPlaced: failed to generate event UUID: " + err.Error())
	}

	orderID, err := uuid.NewV4()
	if err != nil {
		panic("shared.NewOrderPlaced: failed to generate order UUID: " + err.Error())
	}
	return &OrderPlaced{
		EventID:    id,
		OrderID:    orderID,
		CustomerID: customerID,
		Product:    product,
		Amount:     amount,
		OccurredOn: time.Now(),
	}
}

func (e *OrderPlaced) ID() uuid.UUID          { return e.EventID }
func (e *OrderPlaced) AggregateID() uuid.UUID { return e.OrderID }
func (e *OrderPlaced) EventType() string      { return "order.placed" }
func (e *OrderPlaced) OccurredAt() time.Time  { return e.OccurredOn }

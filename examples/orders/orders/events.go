package orders

import "github.com/t3stackcoder/go-api-backend/mediator"

// Consumer groups of the durable consumers. Group names follow
// mediator.NamePattern, which has no hyphen, so the read model group is
// read_model rather than the read-model written in spec 13.
const (
	GroupInventory = "inventory"
	GroupReadModel = "read_model"
)

// Groups lists every consumer group the service registers.
var Groups = []string{GroupInventory, GroupReadModel}

// OrderSubmitted is published by SubmitOrder inside its transaction. It is
// Durable: one outbox row per event, keyed and ordered by the order ID, and
// delivered at least once to every consumer group.
type OrderSubmitted struct {
	mediator.Event

	OrderID    string `json:"orderId"`
	CustomerID string `json:"customerId"`
	Total      int64  `json:"total" doc:"Order total in minor units"`
}

// StreamKey is the order ID: the events of one order stay in order.
func (e OrderSubmitted) StreamKey() string { return e.OrderID }

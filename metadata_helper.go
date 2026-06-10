package eventstore

// BaseEvent is a tiny embeddable helper that supplies a default
// implementation for the helper method of DomainEvent. Embed it in your
// domain events when you do not need custom Metadata behavior:
//
//	type OrderPlaced struct {
//	    eventstore.BaseEvent
//	    OrderID string
//	}
//
// To override the default, define the method on the embedding type
// and Go's method-set rules will pick yours up:
//
//	func (e OrderPlaced) Metadata() eventstore.Metadata {
//	    return eventstore.Metadata{
//	        eventstore.MetadataKeyCorrelationID: e.correlationID,
//	    }
//	}
//
// Because BaseEvent is a value type, embedding it does not allocate and adds
// no fields to the embedding struct.
type BaseEvent struct{}

// Metadata returns nil so events that embed BaseEvent satisfy the DomainEvent
// interface with no cross-cutting metadata.
func (BaseEvent) Metadata() Metadata { return nil }

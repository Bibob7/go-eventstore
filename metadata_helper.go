package eventstore

// BaseEvent is a tiny embeddable helper that satisfies the Metadata() method
// of DomainEvent with a default of nil. Embed it in your domain events when
// you do not need custom metadata behavior:
//
//	type OrderPlaced struct {
//	    eventstore.BaseEvent
//	    OrderID string
//	}
//
// To carry metadata, override the method on the embedding type:
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
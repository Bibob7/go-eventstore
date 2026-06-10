package eventstore

// BaseEvent is a tiny embeddable helper that satisfies MetadataProvider with
// a nil default. Embed it in domain events that opt into metadata support but
// do not carry any by default:
//
//	type OrderPlaced struct {
//	    eventstore.BaseEvent
//	    OrderID string
//	}
//
// To attach metadata, define the method on the embedding type and Go's
// method-set rules will pick yours up:
//
//	func (e OrderPlaced) Metadata() eventstore.Metadata {
//	    return eventstore.Metadata{
//	        eventstore.MetadataKeyCorrelationID: e.correlationID,
//	    }
//	}
//
// Events that never need metadata do not have to embed BaseEvent at all —
// the store checks for MetadataProvider via a type assertion and treats
// absent metadata as equivalent to nil.
//
// Because BaseEvent is a value type, embedding it does not allocate and adds
// no fields to the embedding struct.
type BaseEvent struct{}

// Metadata returns nil, satisfying MetadataProvider with no cross-cutting metadata.
func (BaseEvent) Metadata() Metadata { return nil }

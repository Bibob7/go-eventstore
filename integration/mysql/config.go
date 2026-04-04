package eventstore

// Config holds the configuration for an event store integration.
// All table names are required and must match the schema created
// from the DDL scripts in sql/mysql/schema.sql.
type Config struct {
	// OutboxTableName is the name of the table used to store domain events.
	OutboxTableName string `mapstructure:"outboxTableName"`
	// IncrementIDTableName is the name of the table used to persist relay consumer positions.
	IncrementIDTableName string `mapstructure:"incrementIDTableName"`
}

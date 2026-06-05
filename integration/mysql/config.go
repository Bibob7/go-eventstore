package mysql

// Config holds the configuration for an event store integration.
// All table names are required and must match the schema created
// from the DDL scripts in sql/mysql/schema.sql.
type Config struct {
	// EventStoreTableName is the name of the table used to store domain events.
	EventStoreTableName string `mapstructure:"eventStoreTableName"`
	// IncrementIDTableName is the name of the table used to persist relay positions.
	IncrementIDTableName string `mapstructure:"incrementIDTableName"`
}

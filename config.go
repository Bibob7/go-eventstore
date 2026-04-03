package eventstore

type Config struct {
	OutboxTableName string `mapstructure:"outboxTableName"`
}

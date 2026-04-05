// Package shared provides helpers used across all MySQL integration examples.
package shared

import (
	"database/sql"
	"fmt"
	"os"
	"time"
)

// DSN returns the MySQL data source name from the MYSQL_DSN environment variable,
// or the default that matches the docker-compose setup.
func DSN() string {
	if v := os.Getenv("MYSQL_DSN"); v != "" {
		return v
	}
	return "eventstore:eventstore@tcp(localhost:3306)/eventstore_example?parseTime=true"
}

// WaitForDB retries db.Ping up to 15 times with one-second intervals.
func WaitForDB(db *sql.DB) error {
	for i := range 15 {
		if err := db.Ping(); err == nil {
			return nil
		}
		fmt.Printf("waiting for database... (%d/15)\n", i+1)
		time.Sleep(time.Second)
	}
	return db.Ping()
}

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
	return "eventstore:eventstore@tcp(127.0.0.1:3306)/eventstore_example"
}

// WaitForDB retries db.Ping up to 15 times with one-second intervals.
func WaitForDB(db *sql.DB) error {
	const retries = 15
	var last error
	for i := range retries {
		if last = db.Ping(); last == nil {
			return nil
		}
		fmt.Printf("waiting for database... (%d/%d): %v\n", i+1, retries, last)
		time.Sleep(time.Second)
	}
	return last
}

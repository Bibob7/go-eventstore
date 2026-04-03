module github.com/Bibob7/go-eventstore/integration/mysql

go 1.26

require (
	github.com/Bibob7/go-eventstore v0.0.0
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/go-sql-driver/mysql v1.9.3
	github.com/gofrs/uuid/v5 v5.4.0
	github.com/stretchr/testify v1.11.1
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// replace is used during local development until the core module is published and tagged.
// Remove this directive once github.com/Bibob7/go-eventstore is available on pkg.go.dev.
replace github.com/Bibob7/go-eventstore => ../../


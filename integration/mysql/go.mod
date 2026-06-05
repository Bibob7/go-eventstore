module github.com/Bibob7/go-eventstore/integration/mysql

go 1.26

require (
	github.com/Bibob7/go-eventstore v0.6.0
	github.com/go-sql-driver/mysql v1.10.0
	github.com/gofrs/uuid/v5 v5.4.0
	github.com/stretchr/testify v1.11.1
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// Use the local source tree so integration tests exercise the same code
// that gets shipped. CI runs from the repo root, so the relative path is
// stable. `go mod tidy` will keep the require line above for the module
// graph, but resolution always prefers the replace.
replace github.com/Bibob7/go-eventstore => ../..

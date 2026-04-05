package mysql

import (
	"fmt"
	"regexp"
)

// identifierRegex matches unquoted SQL identifiers: a letter or underscore
// followed by letters, digits, or underscores. This deliberately excludes
// backtick-quoted identifiers and multi-part names (e.g. "db.table") so
// that any value flowing into a fmt.Sprintf-built statement is safe.
var identifierRegex = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// mustValidateIdentifier panics if name is not a valid unquoted SQL identifier.
// Used at construction time to turn SQL-injection risk from table name
// parameters into a fail-fast programmer error.
func mustValidateIdentifier(field, name string) {
	if !identifierRegex.MatchString(name) {
		panic(fmt.Sprintf("mysql: invalid %s %q: must match %s", field, name, identifierRegex.String()))
	}
}

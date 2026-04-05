package mysql

import "testing"

func TestMustValidateIdentifier(t *testing.T) {
	tests := []struct {
		name       string
		identifier string
		wantPanic  bool
	}{
		{name: "simple lowercase", identifier: "outbox", wantPanic: false},
		{name: "with underscore", identifier: "event_increment_id", wantPanic: false},
		{name: "mixed case and digits", identifier: "Outbox123", wantPanic: false},
		{name: "leading underscore", identifier: "_private", wantPanic: false},
		{name: "empty", identifier: "", wantPanic: true},
		{name: "leading digit", identifier: "1outbox", wantPanic: true},
		{name: "contains dot", identifier: "db.outbox", wantPanic: true},
		{name: "contains space", identifier: "out box", wantPanic: true},
		{name: "sql injection attempt", identifier: "outbox; DROP TABLE users--", wantPanic: true},
		{name: "backtick quoted", identifier: "`outbox`", wantPanic: true},
		{name: "hyphen", identifier: "out-box", wantPanic: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if tc.wantPanic && r == nil {
					t.Errorf("expected panic for %q, got none", tc.identifier)
				}
				if !tc.wantPanic && r != nil {
					t.Errorf("unexpected panic for %q: %v", tc.identifier, r)
				}
			}()
			mustValidateIdentifier("tableName", tc.identifier)
		})
	}
}

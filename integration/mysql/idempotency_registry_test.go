package mysql

import (
	"context"
	"database/sql/driver"
	"github.com/Bibob7/go-eventstore"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	msql "github.com/go-sql-driver/mysql"
)

const testTable = "idempotency_registry"

func newRegistryWithMock(t *testing.T) (*idempotencyRegistry, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New(): %v", err)
	}
	reg := &idempotencyRegistry{db: db, tableName: testTable}
	cleanup := func() { db.Close() }
	return reg, mock, cleanup
}

func beginExpect(mock sqlmock.Sqlmock) {
	mock.ExpectBegin()
}

func commitExpect(mock sqlmock.Sqlmock) {
	mock.ExpectCommit()
}

func rollbackExpect(mock sqlmock.Sqlmock) {
	mock.ExpectRollback()
}

func TestRegisterKey_InsertPending_Success(t *testing.T) {
	reg, mock, cleanup := newRegistryWithMock(t)
	defer cleanup()

	payload, namespace := "evt-1:handler-a", "relay-x"
	key := hashKey(payload, namespace)

	beginExpect(mock)
	mock.ExpectExec(regexp.QuoteMeta(
		"INSERT INTO idempotency_registry (idempotency_key, state, created_at, updated_at) VALUES (?, ?, NOW(), NOW()) ON DUPLICATE KEY UPDATE state = IF(state = ?, VALUES(state), state), updated_at = NOW()",
	)).WithArgs(key, statePending, stateFailed).WillReturnResult(sqlmock.NewResult(0, 1))

	rows := sqlmock.NewRows([]string{"state"}).AddRow(statePending)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT state FROM idempotency_registry WHERE idempotency_key = ?")).WithArgs(key).WillReturnRows(rows)
	commitExpect(mock)

	if err := reg.RegisterKey(context.Background(), payload, namespace); err != nil {
		t.Fatalf("RegisterKey err = %v; want nil", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestRegisterKey_WhenSuccess_ReturnAlreadyExist(t *testing.T) {
	reg, mock, cleanup := newRegistryWithMock(t)
	defer cleanup()

	payload, namespace := "evt-2:handler-b", "relay-x"
	key := hashKey(payload, namespace)

	beginExpect(mock)
	mock.ExpectExec(regexp.QuoteMeta(
		"INSERT INTO idempotency_registry (idempotency_key, state, created_at, updated_at) VALUES (?, ?, NOW(), NOW()) ON DUPLICATE KEY UPDATE state = IF(state = ?, VALUES(state), state), updated_at = NOW()",
	)).WithArgs(key, statePending, stateFailed).WillReturnResult(sqlmock.NewResult(0, 1))

	rows := sqlmock.NewRows([]string{"state"}).AddRow(stateSuccess)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT state FROM idempotency_registry WHERE idempotency_key = ?")).WithArgs(key).WillReturnRows(rows)
	rollbackExpect(mock)

	err := reg.RegisterKey(context.Background(), payload, namespace)
	if err == nil || err != eventstore.ErrAlreadyExist {
		t.Fatalf("RegisterKey err = %v; want eventstore.ErrAlreadyExist", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestRegisterKey_WhenFailed_AllowsRetryToPending(t *testing.T) {
	reg, mock, cleanup := newRegistryWithMock(t)
	defer cleanup()

	payload, namespace := "evt-3:handler-c", "relay-x"
	key := hashKey(payload, namespace)

	beginExpect(mock)
	mock.ExpectExec(regexp.QuoteMeta(
		"INSERT INTO idempotency_registry (idempotency_key, state, created_at, updated_at) VALUES (?, ?, NOW(), NOW()) ON DUPLICATE KEY UPDATE state = IF(state = ?, VALUES(state), state), updated_at = NOW()",
	)).WithArgs(key, statePending, stateFailed).WillReturnResult(sqlmock.NewResult(0, 1))

	rows := sqlmock.NewRows([]string{"state"}).AddRow(statePending)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT state FROM idempotency_registry WHERE idempotency_key = ?")).WithArgs(key).WillReturnRows(rows)
	commitExpect(mock)

	if err := reg.RegisterKey(context.Background(), payload, namespace); err != nil {
		t.Fatalf("RegisterKey err = %v; want nil", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestRegisterKey_RetriesOnDeadlockThenSucceeds(t *testing.T) {
	reg, mock, cleanup := newRegistryWithMock(t)
	defer cleanup()

	payload, namespace := "evt-4:handler-d", "relay-x"
	key := hashKey(payload, namespace)

	// First attempt: deadlock during UPSERT → rollback → retry
	beginExpect(mock)
	deadlock := &msql.MySQLError{Number: 1213, Message: "Deadlock found"}
	mock.ExpectExec(regexp.QuoteMeta(
		"INSERT INTO idempotency_registry (idempotency_key, state, created_at, updated_at) VALUES (?, ?, NOW(), NOW()) ON DUPLICATE KEY UPDATE state = IF(state = ?, VALUES(state), state), updated_at = NOW()",
	)).WithArgs(key, statePending, stateFailed).WillReturnError(deadlock)
	rollbackExpect(mock)

	// Second attempt: works
	beginExpect(mock)
	mock.ExpectExec(regexp.QuoteMeta(
		"INSERT INTO idempotency_registry (idempotency_key, state, created_at, updated_at) VALUES (?, ?, NOW(), NOW()) ON DUPLICATE KEY UPDATE state = IF(state = ?, VALUES(state), state), updated_at = NOW()",
	)).WithArgs(key, statePending, stateFailed).WillReturnResult(sqlmock.NewResult(0, 1))

	rows := sqlmock.NewRows([]string{"state"}).AddRow(statePending)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT state FROM idempotency_registry WHERE idempotency_key = ?")).WithArgs(key).WillReturnRows(rows)
	commitExpect(mock)

	start := time.Now()
	if err := reg.RegisterKey(context.Background(), payload, namespace); err != nil {
		t.Fatalf("RegisterKey err = %v; want nil", err)
	}
	if time.Since(start) > time.Second {
		t.Logf("warning: retry took longer than expected")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestRegisterKey_ValidationErrors(t *testing.T) {
	reg, _, cleanup := newRegistryWithMock(t)
	defer cleanup()

	if err := reg.RegisterKey(context.Background(), "", "ns"); err == nil {
		t.Fatalf("expected error for empty payload")
	}
	if err := reg.RegisterKey(context.Background(), "payload", ""); err == nil {
		t.Fatalf("expected error for empty namespace")
	}
}

func TestMarkAsSuccess_UpdateOneRow_OK(t *testing.T) {
	reg, mock, cleanup := newRegistryWithMock(t)
	defer cleanup()

	payload, namespace := "evt-5:handler-e", "relay-x"
	key := hashKey(payload, namespace)

	beginExpect(mock)
	mock.ExpectExec(regexp.QuoteMeta("UPDATE idempotency_registry SET state = ?, updated_at = NOW() WHERE idempotency_key = ?")).
		WithArgs(stateSuccess, key).
		WillReturnResult(sqlmock.NewResult(0, 1))
	commitExpect(mock)

	if err := reg.MarkAsSuccess(context.Background(), payload, namespace); err != nil {
		t.Fatalf("MarkAsSuccess err = %v; want nil", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestMarkAsSuccess_MissingRow_Error(t *testing.T) {
	reg, mock, cleanup := newRegistryWithMock(t)
	defer cleanup()

	payload, namespace := "evt-X:handler-x", "relay-x"
	key := hashKey(payload, namespace)

	beginExpect(mock)
	mock.ExpectExec(regexp.QuoteMeta("UPDATE idempotency_registry SET state = ?, updated_at = NOW() WHERE idempotency_key = ?")).
		WithArgs(stateSuccess, key).
		WillReturnResult(sqlmock.NewResult(0, 0))
	rollbackExpect(mock)

	if err := reg.MarkAsSuccess(context.Background(), payload, namespace); err == nil {
		t.Fatalf("expected error when no rows affected")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestMarkAsFailed_UpdateOneRow_OK(t *testing.T) {
	reg, mock, cleanup := newRegistryWithMock(t)
	defer cleanup()

	payload, namespace := "evt-6:handler-f", "relay-x"
	key := hashKey(payload, namespace)

	beginExpect(mock)
	mock.ExpectExec(regexp.QuoteMeta("UPDATE idempotency_registry SET state = ?, updated_at = NOW() WHERE idempotency_key = ?")).
		WithArgs(stateFailed, key).
		WillReturnResult(sqlmock.NewResult(0, 1))
	commitExpect(mock)

	if err := reg.MarkAsFailed(context.Background(), payload, namespace); err != nil {
		t.Fatalf("MarkAsFailed err = %v; want nil", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestMarkAsFailed_MissingRow_Error(t *testing.T) {
	reg, mock, cleanup := newRegistryWithMock(t)
	defer cleanup()

	payload, namespace := "evt-Y:handler-y", "relay-x"
	key := hashKey(payload, namespace)

	beginExpect(mock)
	mock.ExpectExec(regexp.QuoteMeta("UPDATE idempotency_registry SET state = ?, updated_at = NOW() WHERE idempotency_key = ?")).
		WithArgs(stateFailed, key).
		WillReturnResult(sqlmock.NewResult(0, 0))
	rollbackExpect(mock)

	if err := reg.MarkAsFailed(context.Background(), payload, namespace); err == nil {
		t.Fatalf("expected error when no rows affected")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestEmptyKeyValidation_ForAllMethods(t *testing.T) {
	reg, _, cleanup := newRegistryWithMock(t)
	defer cleanup()

	if err := reg.RegisterKey(context.Background(), "", "ns"); err == nil {
		t.Errorf("RegisterKey: expected error for empty payload")
	}
	if err := reg.MarkAsSuccess(context.Background(), "", "ns"); err == nil {
		t.Errorf("MarkAsSuccess: expected error for empty payload")
	}
	if err := reg.MarkAsFailed(context.Background(), "", "ns"); err == nil {
		t.Errorf("MarkAsFailed: expected error for empty payload")
	}
}

// Ensure the driver.Valuer/Argument types don't break sqlmock expectations
var _ driver.Value = nil

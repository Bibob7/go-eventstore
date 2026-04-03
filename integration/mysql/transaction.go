package mysql

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
)

type txKey struct{}

func WithTx(ctx context.Context, tx *sql.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

func GetTx(ctx context.Context) (*sql.Tx, bool) {
	tx, ok := ctx.Value(txKey{}).(*sql.Tx)
	return tx, ok
}

func WithTransaction(ctx context.Context, db *sql.DB, f func(tx *sql.Tx) error, opts *sql.TxOptions) (retErr error) {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
				slog.Error("rollback after panic failed", "err", rbErr)
			}
			panic(p)
		}

		if retErr != nil {
			if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
				slog.Error("rollback after error failed", "err", rbErr)
			}
			return
		}

		if cmErr := tx.Commit(); cmErr != nil {
			retErr = cmErr
		}
	}()

	retErr = f(tx)
	return retErr
}

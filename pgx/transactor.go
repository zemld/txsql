package pgx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/zemld/txsql"
)

const _rollbackTimeout = 5 * time.Second

type pool interface {
	querier
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

type querier interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type Transactor struct {
	pool     pool
	defaults txsql.TxOptions
}

func New(pool pool, opts ...txsql.TxOption) *Transactor {
	defaults := txsql.TxOptions{}
	for _, option := range opts {
		option(&defaults)
	}

	return &Transactor{pool: pool, defaults: defaults}
}

func (d *Transactor) Do(ctx context.Context, fn func(context.Context) error, opts ...txsql.TxOption) (err error) {
	options := d.defaults
	for _, option := range opts {
		option(&options)
	}

	tx, err := d.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.TxIsoLevel(options.Isolation),
	})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), _rollbackTimeout)
		defer cancel()

		rollbackErr := tx.Rollback(cleanupCtx)
		if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			err = errors.Join(err, fmt.Errorf("rollback transaction: %w", rollbackErr))
		}
	}()

	if err := fn(context.WithValue(ctx, transactionContextKey{}, tx)); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

type transactionContextKey struct{}

func (d *Transactor) getExecutor(ctx context.Context) querier {
	if tx, ok := ctx.Value(transactionContextKey{}).(pgx.Tx); ok {
		return tx
	}

	return d.pool
}

func (d *Transactor) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	tag, err := d.getExecutor(ctx).Exec(ctx, sql, arguments...)
	if err != nil {
		return tag, fmt.Errorf("execute statement: %w", err)
	}

	return tag, nil
}

func (d *Transactor) Query(ctx context.Context, sql string, arguments ...any) (pgx.Rows, error) {
	rows, err := d.getExecutor(ctx).Query(ctx, sql, arguments...)
	if err != nil {
		return rows, fmt.Errorf("query rows: %w", err)
	}

	return rows, nil
}

func (d *Transactor) QueryRow(ctx context.Context, sql string, arguments ...any) pgx.Row {
	return row{source: d.getExecutor(ctx).QueryRow(ctx, sql, arguments...)}
}

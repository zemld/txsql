package pgx

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/zemld/txsql"
	"github.com/zemld/txsql/pgx/mocks"
)

func TestDoBeginFailure(t *testing.T) {
	tests := []struct {
		name  string
		cause error
	}{
		{name: "does not run function when beginning fails", cause: errors.New("connection unavailable")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := mocks.NewMockPool(t)
			pool.EXPECT().BeginTx(mock.Anything, pgx.TxOptions{}).Return(nil, tt.cause).Once()

			called := false
			err := New(pool).Do(t.Context(), func(context.Context) error { called = true; return nil })
			require.ErrorIs(t, err, tt.cause)
			require.ErrorContains(t, err, "begin transaction")
			require.False(t, called)
		})
	}
}

func TestDoCommit(t *testing.T) {
	tests := []struct {
		name      string
		commitErr error
	}{
		{name: "returns nil after successful commit"},
		{name: "preserves commit failure", commitErr: errors.New("commit unavailable")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := mocks.NewMockPool(t)
			tx := mocks.NewMockTx(t)
			pool.EXPECT().BeginTx(mock.Anything, pgx.TxOptions{}).Return(tx, nil).Once()
			tx.EXPECT().Commit(mock.Anything).Return(tt.commitErr).Once()
			tx.EXPECT().Rollback(mock.Anything).Return(pgx.ErrTxClosed).Once()

			calls := 0
			err := New(pool).Do(t.Context(), func(context.Context) error { calls++; return nil })
			require.Equal(t, 1, calls)
			require.ErrorIs(t, err, tt.commitErr)

			if tt.commitErr != nil {
				require.ErrorContains(t, err, "commit transaction")
			}
		})
	}
}

func TestDoRollback(t *testing.T) {
	callbackErr := &pgconn.PgError{Code: "40001", Message: "serialization failure"}
	rollbackErr := errors.New("rollback unavailable")

	tests := []struct {
		name        string
		cancel      bool
		panicValue  any
		rollbackErr error
	}{
		{name: "preserves callback error without retry"},
		{name: "preserves both callback and rollback errors", rollbackErr: rollbackErr},
		{name: "cleans up after context cancellation", cancel: true},
		{name: "preserves panic", panicValue: new("original panic")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := mocks.NewMockPool(t)
			tx := mocks.NewMockTx(t)
			pool.EXPECT().BeginTx(mock.Anything, pgx.TxOptions{}).Return(tx, nil).Once()
			tx.EXPECT().Rollback(mock.MatchedBy(func(ctx context.Context) bool {
				deadline, ok := ctx.Deadline()
				return ctx.Err() == nil && ok && time.Until(deadline) > 0 && time.Until(deadline) <= 5*time.Second
			})).Return(tt.rollbackErr).Once()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			calls := 0
			run := func() error {
				return New(pool).Do(ctx, func(context.Context) error {
					calls++

					if tt.cancel {
						cancel()
					}

					if tt.panicValue != nil {
						panic(tt.panicValue)
					}

					return callbackErr
				})
			}

			if tt.panicValue != nil {
				require.PanicsWithValue(t, tt.panicValue, func() { _ = run() })
			} else {
				err := run()
				require.ErrorIs(t, err, callbackErr)
				pgErr, ok := errors.AsType[*pgconn.PgError](err)
				require.True(t, ok)
				require.Equal(t, "40001", pgErr.Code)

				if tt.rollbackErr != nil {
					require.ErrorIs(t, err, tt.rollbackErr)
				}
			}

			require.Equal(t, 1, calls)
		})
	}
}

func TestDoOptions(t *testing.T) {
	tests := []struct {
		name         string
		defaults     []txsql.TxOption
		overrides    []txsql.TxOption
		want         pgx.TxOptions
		wantDefaults pgx.TxOptions
	}{
		{name: "uses PostgreSQL defaults"},
		{
			name:         "uses configured defaults",
			defaults:     []txsql.TxOption{txsql.WithIsolation(txsql.Serializable)},
			want:         pgx.TxOptions{IsoLevel: pgx.Serializable},
			wantDefaults: pgx.TxOptions{IsoLevel: pgx.Serializable},
		},
		{
			name:         "overrides isolation without changing defaults",
			defaults:     []txsql.TxOption{txsql.WithIsolation(txsql.Serializable)},
			overrides:    []txsql.TxOption{txsql.WithIsolation(txsql.RepeatableRead)},
			want:         pgx.TxOptions{IsoLevel: pgx.RepeatableRead},
			wantDefaults: pgx.TxOptions{IsoLevel: pgx.Serializable},
		},
		{
			name:         "last override wins without changing defaults",
			defaults:     []txsql.TxOption{txsql.WithIsolation(txsql.Serializable)},
			overrides:    []txsql.TxOption{txsql.WithIsolation(txsql.ReadUncommitted), txsql.WithIsolation(txsql.ReadCommitted)},
			want:         pgx.TxOptions{IsoLevel: pgx.ReadCommitted},
			wantDefaults: pgx.TxOptions{IsoLevel: pgx.Serializable},
		},
		{
			name:         "last constructor option wins",
			defaults:     []txsql.TxOption{txsql.WithIsolation(txsql.Serializable), txsql.WithIsolation(txsql.ReadCommitted)},
			want:         pgx.TxOptions{IsoLevel: pgx.ReadCommitted},
			wantDefaults: pgx.TxOptions{IsoLevel: pgx.ReadCommitted},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := mocks.NewMockPool(t)
			for _, want := range []pgx.TxOptions{tt.want, tt.wantDefaults} {
				tx := mocks.NewMockTx(t)
				pool.EXPECT().BeginTx(mock.Anything, want).Return(tx, nil).Once()
				tx.EXPECT().Commit(mock.Anything).Return(nil).Once()
				tx.EXPECT().Rollback(mock.Anything).Return(pgx.ErrTxClosed).Once()
			}

			var transactor txsql.Transactor = New(pool, tt.defaults...)
			require.NoError(t, transactor.Do(t.Context(), func(context.Context) error { return nil }, tt.overrides...))
			require.NoError(t, transactor.Do(t.Context(), func(context.Context) error { return nil }))
		})
	}
}

func TestTransactorRegistrationRecord(t *testing.T) {
	url := os.Getenv("DATABASE_TEST_URL")
	if url == "" {
		t.Skip("DATABASE_TEST_URL is not set")
	}

	tests := []struct {
		name          string
		failMark      bool
		wantPlayers   int
		wantProcessed bool
	}{
		{name: "adds lost player and marks registration record processed", wantPlayers: 1, wantProcessed: true},
		{name: "rolls back lost player when marking registration record fails", failMark: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := pgxpool.ParseConfig(url)
			require.NoError(t, err)

			config.MaxConns = 1
			pool, err := pgxpool.NewWithConfig(t.Context(), config)
			require.NoError(t, err)

			defer pool.Close()

			db := New(pool)
			_, err = db.Exec(t.Context(), `CREATE TEMP TABLE lost_players (id integer PRIMARY KEY); CREATE TEMP TABLE registration_records (id integer PRIMARY KEY, processed boolean NOT NULL DEFAULT false); INSERT INTO registration_records (id) VALUES (1)`)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			err = db.Do(ctx, func(ctx context.Context) error {
				_, err := db.Exec(ctx, `INSERT INTO lost_players (id) VALUES ($1)`, 1)
				if err != nil {
					return err
				}

				var id int
				if err := db.QueryRow(ctx, `SELECT id FROM lost_players`).Scan(&id); err != nil {
					return fmt.Errorf("read added lost player: %w", err)
				}

				require.Equal(t, 1, id)

				rows, err := db.Query(ctx, `SELECT processed FROM registration_records WHERE id = $1`, 1)
				if err != nil {
					return err
				}

				require.True(t, rows.Next())

				var processed bool
				require.NoError(t, rows.Scan(&processed))
				require.False(t, processed)
				rows.Close()

				if tt.failMark {
					_, err = db.Exec(ctx, `UPDATE registration_records SET processed = NULL WHERE id = $1`, 1)
					return err
				}

				_, err = db.Exec(ctx, `UPDATE registration_records SET processed = true WHERE id = $1`, 1)

				return err
			})
			if tt.failMark {
				pgErr, ok := errors.AsType[*pgconn.PgError](err)
				require.True(t, ok)
				require.Equal(t, "23502", pgErr.Code)
			} else {
				require.NoError(t, err)
			}

			var players int
			require.NoError(t, db.QueryRow(t.Context(), `SELECT count(*) FROM lost_players`).Scan(&players))
			require.Equal(t, tt.wantPlayers, players)
			rows, err := db.Query(t.Context(), `SELECT processed FROM registration_records WHERE id = $1`, 1)
			require.NoError(t, err)

			defer rows.Close()

			require.True(t, rows.Next())

			var processed bool
			require.NoError(t, rows.Scan(&processed))
			require.Equal(t, tt.wantProcessed, processed)
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		})
	}
}

func TestTransactorIsolation(t *testing.T) {
	url := os.Getenv("DATABASE_TEST_URL")
	if url == "" {
		t.Skip("DATABASE_TEST_URL is not set")
	}

	tests := []struct {
		name          string
		options       []txsql.TxOption
		wantIsolation string
	}{
		{name: "uses configured serializable isolation", wantIsolation: "serializable"},
		{name: "overrides to repeatable read", options: []txsql.TxOption{txsql.WithIsolation(txsql.RepeatableRead)}, wantIsolation: "repeatable read"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool, err := pgxpool.New(t.Context(), url)
			require.NoError(t, err)

			defer pool.Close()

			db := New(pool, txsql.WithIsolation(txsql.Serializable))

			var isolation string

			err = db.Do(t.Context(), func(ctx context.Context) error {
				return db.QueryRow(ctx, `SHOW transaction_isolation`).Scan(&isolation)
			}, tt.options...)
			require.NoError(t, err)
			require.Equal(t, tt.wantIsolation, isolation)
		})
	}
}

func TestTransactorCleanup(t *testing.T) {
	url := os.Getenv("DATABASE_TEST_URL")
	if url == "" {
		t.Skip("DATABASE_TEST_URL is not set")
	}

	tests := []struct {
		name       string
		panicValue any
		cancel     bool
	}{
		{name: "rolls back after cancellation and releases connection", cancel: true},
		{name: "rolls back after panic and releases connection", panicValue: new("original panic")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := pgxpool.ParseConfig(url)
			require.NoError(t, err)

			config.MaxConns = 1
			pool, err := pgxpool.NewWithConfig(t.Context(), config)
			require.NoError(t, err)

			defer pool.Close()

			db := New(pool)
			_, err = db.Exec(t.Context(), `CREATE TEMP TABLE lost_players (id integer)`)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			run := func() error {
				return db.Do(ctx, func(ctx context.Context) error {
					if _, err := db.Exec(ctx, `INSERT INTO lost_players (id) VALUES (1)`); err != nil {
						return err
					}

					if tt.cancel {
						cancel()
						return ctx.Err()
					}

					panic(tt.panicValue)
				})
			}

			if tt.panicValue != nil {
				require.PanicsWithValue(t, tt.panicValue, func() { _ = run() })
			} else {
				require.ErrorIs(t, run(), context.Canceled)
			}

			checkCtx, checkCancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer checkCancel()

			var count int
			require.NoError(t, db.QueryRow(checkCtx, `SELECT count(*) FROM lost_players`).Scan(&count))
			require.Zero(t, count)
		})
	}
}

func TestTransactorQueryRow(t *testing.T) {
	tests := []struct {
		name    string
		scanErr error
	}{
		{name: "preserves successful scan"},
		{name: "wraps missing row error", scanErr: pgx.ErrNoRows},
		{name: "wraps PostgreSQL error", scanErr: &pgconn.PgError{Code: "42703", Message: "undefined column"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := mocks.NewMockPool(t)
			row := mocks.NewMockRow(t)

			var value int

			pool.EXPECT().QueryRow(mock.Anything, "SELECT value FROM records WHERE id = $1", []any{1}).Return(row).Once()
			row.EXPECT().Scan([]any{&value}).Return(tt.scanErr).Once()
			err := New(pool).QueryRow(t.Context(), "SELECT value FROM records WHERE id = $1", 1).Scan(&value)
			require.ErrorIs(t, err, tt.scanErr)

			if tt.scanErr != nil {
				require.ErrorContains(t, err, "query row: ")
			}

			if want, ok := errors.AsType[*pgconn.PgError](tt.scanErr); ok {
				got, ok := errors.AsType[*pgconn.PgError](err)
				require.True(t, ok)
				require.Same(t, want, got)
			}
		})
	}
}

func TestTransactorQueryRowPostgresErrors(t *testing.T) {
	url := os.Getenv("DATABASE_TEST_URL")
	if url == "" {
		t.Skip("DATABASE_TEST_URL is not set")
	}

	tests := []struct {
		name     string
		query    string
		wantErr  error
		wantCode string
	}{
		{name: "wraps no rows from PostgreSQL", query: "SELECT 1 WHERE false", wantErr: pgx.ErrNoRows},
		{name: "wraps query failure from PostgreSQL", query: "SELECT missing_column", wantCode: "42703"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool, err := pgxpool.New(t.Context(), url)
			require.NoError(t, err)

			defer pool.Close()

			var value int

			err = New(pool).QueryRow(t.Context(), tt.query).Scan(&value)
			require.ErrorContains(t, err, "query row: ")

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			}

			if tt.wantCode != "" {
				pgErr, ok := errors.AsType[*pgconn.PgError](err)
				require.True(t, ok)
				require.Equal(t, tt.wantCode, pgErr.Code)
			}
		})
	}
}

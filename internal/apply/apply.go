// Package apply executes delta SQL against the user's own database.
//
// Deltas carry ready-to-run multi-statement SQL rendered server-side; every
// mapped table has a _sync_revision guard, so re-applying a delta is
// physically harmless. Drivers per dialect:
//
//	postgres → jackc/pgx (simple protocol, allows multi-statement Exec)
//	mysql    → go-sql-driver/mysql (multiStatements forced on)
//	sqlite   → modernc.org/sqlite (pure Go; multi-statement natively)
package apply

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"

	"github.com/TOT-Concept/ee-database/internal/framing"
)

// Open connects to the target database for the given Entity Enricher dialect.
func Open(dialect, dsn string) (*sql.DB, error) {
	switch dialect {
	case "", "postgres":
		cfg, err := pgx.ParseConfig(dsn)
		if err != nil {
			return nil, fmt.Errorf("postgres dsn: %w", err)
		}
		// Simple protocol so one Exec can run a delta's multi-statement SQL.
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		return sql.Open("pgx", stdlib.RegisterConnConfig(cfg))
	case "mysql":
		cfg, err := gomysql.ParseDSN(dsn)
		if err != nil {
			return nil, fmt.Errorf("mysql dsn: %w", err)
		}
		cfg.MultiStatements = true
		connector, err := gomysql.NewConnector(cfg)
		if err != nil {
			return nil, fmt.Errorf("mysql: %w", err)
		}
		return sql.OpenDB(connector), nil
	case "sqlite":
		return sql.Open("sqlite", dsn)
	default:
		return nil, fmt.Errorf("unsupported dialect %q", dialect)
	}
}

// Snapshot applies the bootstrap .sql script. The script carries its own
// BEGIN/COMMIT, and every statement is idempotent (CREATE IF NOT EXISTS +
// revision-guarded upserts), so applying it onto a non-empty replica converges.
//
// It runs on a dedicated connection that is discarded on failure: a failed
// multi-statement script can leave the session inside an aborted transaction
// (pgx < v5.8 pooled such connections, so every retry reported SQLSTATE 25P02
// instead of the root cause), and no dialect is trusted to clean that up.
func Snapshot(ctx context.Context, db *sql.DB, sqlText string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	_, execErr := conn.ExecContext(ctx, sqlText)
	if execErr != nil {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
	if closeErr := conn.Close(); execErr == nil {
		return closeErr
	}
	return execErr
}

// Batch applies deltas in id order inside one transaction.
//
// On failure it rolls back and returns the offending delta's id — nothing from
// the batch is committed, the server's lease expires, and the same window is
// redelivered after the cause is fixed.
func Batch(ctx context.Context, db *sql.DB, deltas []framing.Delta) (lastID int64, failedID *int64, err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, nil, fmt.Errorf("begin: %w", err)
	}
	for i := range deltas {
		d := deltas[i]
		if _, err := tx.ExecContext(ctx, d.SQL); err != nil {
			_ = tx.Rollback()
			return 0, &d.ID, fmt.Errorf("delta %d (%s %s): %w", d.ID, d.Kind, d.Op, err)
		}
		lastID = d.ID
	}
	if err := tx.Commit(); err != nil {
		return 0, nil, fmt.Errorf("commit: %w", err)
	}
	return lastID, nil, nil
}

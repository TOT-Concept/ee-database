package apply

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

// EnsureResult reports what EnsureDatabase actually created.
type EnsureResult struct {
	DatabaseCreated bool
	RoleCreated     bool
	Name            string
}

// EnsureDatabase creates the database named in the DSN when it does not exist
// yet (the `run --create-missing` flag). Without adminDSN the DSN's own
// credentials are used, so they need server-level create rights (postgres:
// CREATEDB via the `postgres` maintenance database; mysql: the CREATE
// privilege). With adminDSN (`run --admin-dsn`) the admin connection
// bootstraps everything the target DSN names: the role/user (with the target
// DSN's password) when it does not exist, then the database owned by it.
// SQLite needs nothing — the driver creates the file on first open.
func EnsureDatabase(ctx context.Context, dialect, dsn, adminDSN string) (EnsureResult, error) {
	switch dialect {
	case "", "postgres":
		return ensurePostgres(ctx, dsn, adminDSN)
	case "mysql":
		return ensureMySQL(ctx, dsn, adminDSN)
	case "sqlite":
		return EnsureResult{}, nil
	default:
		return EnsureResult{}, fmt.Errorf("unsupported dialect %q", dialect)
	}
}

func ensurePostgres(ctx context.Context, dsn, adminDSN string) (EnsureResult, error) {
	res := EnsureResult{}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return res, fmt.Errorf("postgres dsn: %w", err)
	}
	res.Name = cfg.Database
	if res.Name == "" {
		return res, errors.New("postgres dsn names no database")
	}
	// CREATE DATABASE cannot target the database being created — go through a
	// maintenance connection: the admin DSN when given (it can also create the
	// target role), else the standard `postgres` database with the target's
	// own credentials.
	maintDSN := dsn
	if adminDSN != "" {
		maintDSN = adminDSN
	}
	maint, err := pgx.ParseConfig(maintDSN)
	if err != nil {
		return res, fmt.Errorf("postgres admin dsn: %w", err)
	}
	if adminDSN == "" || maint.Database == "" {
		maint.Database = "postgres"
	}
	db := sql.OpenDB(stdlib.GetConnector(*maint))
	defer db.Close()

	if adminDSN != "" && cfg.User != "" && cfg.User != maint.User {
		var roleExists bool
		if err := db.QueryRowContext(ctx,
			"SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", cfg.User,
		).Scan(&roleExists); err != nil {
			return res, fmt.Errorf("check role %q: %w", cfg.User, err)
		}
		if !roleExists {
			stmt := "CREATE ROLE " + pgQuoteIdent(cfg.User) + " LOGIN"
			if cfg.Password != "" {
				stmt += " PASSWORD " + pgQuoteLiteral(cfg.Password)
			}
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				// 42710 duplicate_object: concurrent creation — goal met.
				var pgErr *pgconn.PgError
				if !errors.As(err, &pgErr) || pgErr.Code != "42710" {
					return res, fmt.Errorf("create role %q: %w", cfg.User, err)
				}
			} else {
				res.RoleCreated = true
			}
		}
	}

	var exists bool
	if err := db.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", res.Name,
	).Scan(&exists); err != nil {
		return res, fmt.Errorf("check %q via maintenance database: %w", res.Name, err)
	}
	if exists {
		return res, nil
	}
	stmt := "CREATE DATABASE " + pgQuoteIdent(res.Name)
	if adminDSN != "" && cfg.User != "" {
		stmt += " OWNER " + pgQuoteIdent(cfg.User)
	}
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		// 42P04 duplicate_database: another process created it between the
		// check and the create — the goal is met either way.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P04" {
			return res, nil
		}
		return res, fmt.Errorf("create database %q: %w", res.Name, err)
	}
	res.DatabaseCreated = true
	return res, nil
}

func ensureMySQL(ctx context.Context, dsn, adminDSN string) (EnsureResult, error) {
	res := EnsureResult{}
	cfg, err := gomysql.ParseDSN(dsn)
	if err != nil {
		return res, fmt.Errorf("mysql dsn: %w", err)
	}
	res.Name = cfg.DBName
	if res.Name == "" {
		return res, errors.New("mysql dsn names no database")
	}
	maint := cfg.Clone()
	if adminDSN != "" {
		if maint, err = gomysql.ParseDSN(adminDSN); err != nil {
			return res, fmt.Errorf("mysql admin dsn: %w", err)
		}
	}
	maint.DBName = ""
	connector, err := gomysql.NewConnector(maint)
	if err != nil {
		return res, fmt.Errorf("mysql: %w", err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	out, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+mysqlQuoteIdent(res.Name))
	if err != nil {
		return res, fmt.Errorf("create database %q: %w", res.Name, err)
	}
	affected, _ := out.RowsAffected()
	res.DatabaseCreated = affected > 0

	if adminDSN != "" && cfg.User != "" && cfg.User != maint.User {
		var userExists bool
		if err := db.QueryRowContext(ctx,
			"SELECT EXISTS (SELECT 1 FROM mysql.user WHERE user = ?)", cfg.User,
		).Scan(&userExists); err != nil {
			return res, fmt.Errorf("check user %q: %w", cfg.User, err)
		}
		if !userExists {
			stmt := "CREATE USER IF NOT EXISTS " + mysqlQuoteAccount(cfg.User) +
				" IDENTIFIED BY " + mysqlQuoteString(cfg.Passwd)
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				return res, fmt.Errorf("create user %q: %w", cfg.User, err)
			}
			res.RoleCreated = true
		}
		grant := "GRANT ALL PRIVILEGES ON " + mysqlQuoteIdent(res.Name) + ".* TO " +
			mysqlQuoteAccount(cfg.User)
		if _, err := db.ExecContext(ctx, grant); err != nil {
			return res, fmt.Errorf("grant on %q to %q: %w", res.Name, cfg.User, err)
		}
	}
	return res, nil
}

func pgQuoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// pgQuoteLiteral quotes a string literal for statements that cannot take bind
// parameters (CREATE ROLE ... PASSWORD): double every single quote.
func pgQuoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func mysqlQuoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// mysqlQuoteAccount renders 'user'@'%' for CREATE USER / GRANT statements.
func mysqlQuoteAccount(user string) string {
	return mysqlQuoteString(user) + "@'%'"
}

// mysqlQuoteString quotes a MySQL string literal (single quotes doubled,
// backslashes escaped — MySQL treats backslash as an escape by default).
func mysqlQuoteString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "'", "''")
	return "'" + s + "'"
}

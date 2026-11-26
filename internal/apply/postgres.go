package apply

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/TOT-Concept/ee-database/internal/framing"
)

// postgresDriver targets PostgreSQL through jackc/pgx.
type postgresDriver struct{}

func (postgresDriver) Dialect() string  { return "postgres" }
func (postgresDriver) Label() string    { return "PostgreSQL" }
func (postgresDriver) DefaultPort() int { return 5432 }

func (postgresDriver) connectProblem(err error) ConnectProblem {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return ProblemUnknown
	}
	switch pgErr.Code {
	case "28P01", "28000": // invalid_password, invalid_authorization_specification
		return ProblemLogin
	case "3D000", "42501": // invalid_catalog_name, insufficient_privilege (CONNECT)
		return ProblemDatabase
	}
	return ProblemUnknown
}

func (postgresDriver) ComposeDSN(p DSNParts) string {
	u := url.URL{Scheme: "postgres", User: p.userinfo(), Host: p.hostPort(), Path: "/" + p.Database}
	return u.String()
}

// postgresConfig reads a PostgreSQL DSN. pgx takes whatever is not a
// postgres:// URL for keyword=value pairs, so the DSN of another database type
// would be read as no setting at all and open the machine's default server:
// it is refused instead.
func postgresConfig(dsn string) (*pgx.ConnConfig, error) {
	scheme, _, isURL := strings.Cut(dsn, "://")
	if isURL && scheme != "postgres" && scheme != "postgresql" && !strings.Contains(scheme, "=") {
		return nil, fmt.Errorf("a %s:// URL is not a PostgreSQL one "+
			"(postgres://user:pass@host:port/name)", scheme)
	}
	return pgx.ParseConfig(dsn)
}

func (postgresDriver) Open(dsn string) (*sql.DB, error) {
	cfg, err := postgresConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres dsn: %w", err)
	}
	// Simple protocol so one Exec can run a delta's multi-statement SQL.
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	return sql.Open("pgx", stdlib.RegisterConnConfig(cfg))
}

func (postgresDriver) DeriveDSN(baseDSN, databaseName string) (string, error) {
	if databaseName == "" {
		return "", errors.New("empty database name")
	}
	// The parse error is not wrapped: it quotes the DSN, password included.
	u, err := url.Parse(strings.TrimSpace(baseDSN))
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return "", errors.New("postgres base dsn must be a URL (postgres://user:pass@host:port/)")
	}
	u.Path = "/" + databaseName
	return u.String(), nil
}

func (postgresDriver) EnsureDatabase(ctx context.Context, dsn, adminDSN string) (EnsureResult, error) {
	res := EnsureResult{}
	cfg, err := postgresConfig(dsn)
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
	// own credentials (which then need CREATEDB).
	maintDSN := dsn
	if adminDSN != "" {
		maintDSN = adminDSN
	}
	maint, err := postgresConfig(maintDSN)
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
				res.LoginCreated = true
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

func (postgresDriver) Preflight(ctx context.Context, dsn string, db *sql.DB) framing.PreflightReport {
	report := framing.PreflightReport{Dialect: "postgres"}
	if cfg, err := postgresConfig(dsn); err == nil {
		report.User, report.Database = cfg.User, cfg.Database
	}
	report.CreateDB = pgCreateDB(ctx, db, &report.Details)
	report.Trigram = probeTrigram(ctx, db, &report.Details)
	report.DDL, report.DML = probeDDLDML(ctx, db,
		"CREATE TABLE IF NOT EXISTS "+probeTable+" (id integer)", &report.Details)
	return report
}

func (postgresDriver) HostPreflight(ctx context.Context, baseDSN, adminDSN string) framing.PreflightReport {
	report := framing.PreflightReport{Dialect: "postgres"}
	dsn, label := provisioningDSN(baseDSN, adminDSN, &report.Details)
	cfg, err := postgresConfig(dsn)
	if err != nil {
		report.Details = append(report.Details, fmt.Sprintf("%s: %v", label, err))
		return report
	}
	report.User = cfg.User
	if cfg.Database == "" {
		cfg.Database = "postgres"
	}
	db := sql.OpenDB(stdlib.GetConnector(*cfg))
	defer db.Close()
	report.CreateDB = pgCreateDB(ctx, db, &report.Details)
	if report.CreateDB != nil && !*report.CreateDB {
		report.Details = append(report.Details,
			"grant the create-database right with: ALTER ROLE "+pgQuoteIdent(cfg.User)+" CREATEDB")
	}
	return report
}

func (postgresDriver) Snapshot(ctx context.Context, db *sql.DB, script string) error {
	// The whole script is one simple-protocol Exec: it carries its own
	// BEGIN/COMMIT.
	return onDedicatedConn(ctx, db, func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, script)
		return err
	})
}

// DescribeError adds the PostgreSQL fields Error() drops (detail, hint,
// context, table/column/constraint) and, when the server reports an error
// cursor, the offending line of the script.
func (postgresDriver) DescribeError(script string, err error) string {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err.Error()
	}
	var b strings.Builder
	b.WriteString(err.Error())
	describeField(&b, "detail", pgErr.Detail)
	describeField(&b, "hint", pgErr.Hint)
	describeField(&b, "context", pgErr.Where)
	describeField(&b, "table", pgErr.TableName)
	describeField(&b, "column", pgErr.ColumnName)
	describeField(&b, "constraint", pgErr.ConstraintName)
	if pgErr.Position > 0 {
		line, text := locateInScript(script, int(pgErr.Position))
		describeField(&b, fmt.Sprintf("script line %d", line), text)
	}
	return b.String()
}

// locateInScript maps PostgreSQL's 1-based error cursor — measured in
// characters, not bytes — to the number and text of the script line holding it.
func locateInScript(sqlText string, pos int) (int, string) {
	line, lineStart, chars := 1, 0, 0
	for i, r := range sqlText {
		chars++
		if chars == pos {
			end := strings.IndexByte(sqlText[i:], '\n')
			if end < 0 {
				end = len(sqlText)
			} else {
				end += i
			}
			return line, strings.TrimSpace(sqlText[lineStart:end])
		}
		if r == '\n' {
			line++
			lineStart = i + 1
		}
	}
	return line, strings.TrimSpace(sqlText[lineStart:])
}

// pgCreateDB reads the connected role's right to create databases (CREATEDB
// or superuser). CREATE DATABASE is non-transactional, so this reads the
// catalog rather than probing.
func pgCreateDB(ctx context.Context, db *sql.DB, details *[]string) *bool {
	var createDB bool
	if err := db.QueryRowContext(ctx,
		"SELECT rolcreatedb OR rolsuper FROM pg_roles WHERE rolname = current_user",
	).Scan(&createDB); err != nil {
		*details = append(*details, fmt.Sprintf("CREATEDB check failed: %v", err))
		return nil
	}
	return &createDB
}

// probeTrigram checks for the pg_trgm extension and, when absent, tries to
// install it right here — a credential the consumer granted owner rights just
// works, with no ceremony. The refusal lands in details rather than an error:
// search-index DDL is self-guarded server-side (a DO block gated on
// pg_extension), so a missing extension only means those indexes no-op until
// a database owner runs CREATE EXTENSION pg_trgm.
func probeTrigram(ctx context.Context, db *sql.DB, details *[]string) *bool {
	var present bool
	if err := db.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm')",
	).Scan(&present); err != nil {
		*details = append(*details, fmt.Sprintf("pg_trgm check failed: %v", err))
		return nil
	}
	if !present {
		if _, err := db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS pg_trgm"); err == nil {
			present = true
			*details = append(*details, "pg_trgm installed by this client")
		} else {
			*details = append(*details,
				fmt.Sprintf("pg_trgm not installed and CREATE EXTENSION was refused: %v", err))
		}
	}
	return &present
}

func pgQuoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// pgQuoteLiteral quotes a string literal for statements that cannot take bind
// parameters (CREATE ROLE ... PASSWORD): double every single quote.
func pgQuoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

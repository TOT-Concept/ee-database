package apply

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	mssql "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/msdsn"

	"github.com/TOT-Concept/ee-database/internal/framing"
)

// sqlServerDriver targets SQL Server 2022 or later through
// microsoft/go-mssqldb. An Exec without arguments is sent as one T-SQL batch,
// so a delta's multi-statement script needs no special mode.
type sqlServerDriver struct{}

func (sqlServerDriver) Dialect() string  { return "sqlserver" }
func (sqlServerDriver) Label() string    { return "SQL Server" }
func (sqlServerDriver) DefaultPort() int { return 1433 }

// connectProblem: a database the login cannot open is reported as 4060, or
// 4063 when the login falls back to its default database, alongside the
// generic 18456 login failure, so those are looked for first.
func (sqlServerDriver) connectProblem(err error) ConnectProblem {
	var dbErr mssql.Error
	if !errors.As(err, &dbErr) {
		return ProblemUnknown
	}
	all := append([]mssql.Error{dbErr}, dbErr.All...)
	for _, e := range all {
		if e.Number == 4060 || e.Number == 4063 { // cannot open the requested database
			return ProblemDatabase
		}
	}
	for _, e := range all {
		if e.Number == 18456 { // login failed
			return ProblemLogin
		}
	}
	return ProblemUnknown
}

func (sqlServerDriver) ComposeDSN(p DSNParts) string {
	u := url.URL{Scheme: "sqlserver", User: p.userinfo(), Host: p.hostPort(),
		RawQuery: url.Values{"database": {p.Database}}.Encode()}
	return u.String()
}

// The two shapes of a SQL Server DSN, as error messages show them: a target
// names its database, a server (a host's base DSN, an admin DSN) names none.
const (
	msTargetForm = "sqlserver://user:pass@host:port?database=name"
	msServerForm = "sqlserver://user:pass@host:port"
)

// sqlServerConfig reads a DSN in the one form this client documents; form is
// the shape the caller expects, quoted when the DSN is not a URL. The driver
// also accepts ADO and ODBC strings; they are refused so that DeriveDSN, which
// rewrites the URL, and Open agree on what a DSN is.
func sqlServerConfig(dsn, form string) (msdsn.Config, error) {
	dsn = strings.TrimSpace(dsn)
	if !strings.HasPrefix(dsn, "sqlserver://") {
		return msdsn.Config{}, errors.New("it must be a URL (" + form + ")")
	}
	return msdsn.Parse(dsn)
}

// sqlServerTarget reads the DSN of a target database. One that names no
// database is refused: the server would open the login's default database,
// usually master, and the replica would be written there. The PostgreSQL way
// of writing it (sqlserver://host/name) does not name one either.
func sqlServerTarget(dsn string) (msdsn.Config, error) {
	cfg, err := sqlServerConfig(dsn, msTargetForm)
	if err != nil {
		return cfg, fmt.Errorf("sqlserver dsn: %w", err)
	}
	if cfg.Database == "" {
		return cfg, errors.New("sqlserver dsn names no database: add ?database=name " +
			"(a path after the host is an instance name)")
	}
	return cfg, nil
}

func (sqlServerDriver) Open(dsn string) (*sql.DB, error) {
	cfg, err := sqlServerTarget(dsn)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(mssql.NewConnectorConfig(cfg)), nil
}

func (sqlServerDriver) DeriveDSN(baseDSN, databaseName string) (string, error) {
	if databaseName == "" {
		return "", errors.New("empty database name")
	}
	// The parse error is not wrapped: it quotes the DSN, password included.
	u, err := url.Parse(strings.TrimSpace(baseDSN))
	if err != nil || u.Scheme != "sqlserver" {
		return "", errors.New("sqlserver base dsn must be a URL (" + msServerForm + ")")
	}
	query := u.Query()
	for key := range query {
		// The driver reads its parameters case-insensitively.
		if strings.EqualFold(key, "database") {
			query.Del(key)
		}
	}
	query.Set("database", databaseName)
	u.RawQuery = query.Encode()
	return u.String(), nil
}

func (sqlServerDriver) EnsureDatabase(ctx context.Context, dsn, adminDSN string) (EnsureResult, error) {
	res := EnsureResult{}
	cfg, err := sqlServerTarget(dsn)
	if err != nil {
		return res, err
	}
	res.Name = cfg.Database
	// CREATE DATABASE runs from master: through the admin DSN when given (it
	// can also create the target login), else with the target's own
	// credentials (which then need CREATE ANY DATABASE).
	maint := cfg
	if adminDSN != "" {
		if maint, err = sqlServerConfig(adminDSN, msServerForm); err != nil {
			return res, fmt.Errorf("sqlserver admin dsn: %w", err)
		}
	}
	// A database the admin creates belongs to the admin: the login the client
	// runs as has to be let in.
	provision := adminDSN != "" && cfg.User != "" && !strings.EqualFold(cfg.User, maint.User)
	maint.Database = "master"
	master := sql.OpenDB(mssql.NewConnectorConfig(maint))
	defer master.Close()

	if provision {
		var known bool
		if err := master.QueryRowContext(ctx,
			"SELECT CAST(CASE WHEN SUSER_ID(@p1) IS NULL THEN 0 ELSE 1 END AS BIT)", cfg.User,
		).Scan(&known); err != nil {
			return res, fmt.Errorf("check login %q: %w", cfg.User, err)
		}
		if !known {
			// The server's password policy applies (a weak password is error
			// 15118): its message is reported as it is.
			stmt := "CREATE LOGIN " + msQuoteIdent(cfg.User) +
				" WITH PASSWORD = " + msQuoteLiteral(cfg.Password)
			if _, err := master.ExecContext(ctx, stmt); err != nil {
				// 15025: the login exists — concurrent creation, goal met.
				if !isSQLServerError(err, 15025) {
					return res, fmt.Errorf("create login %q: %w", cfg.User, err)
				}
			} else {
				res.LoginCreated = true
			}
		}
	}

	var exists bool
	if err := master.QueryRowContext(ctx,
		"SELECT CAST(CASE WHEN DB_ID(@p1) IS NULL THEN 0 ELSE 1 END AS BIT)", res.Name,
	).Scan(&exists); err != nil {
		return res, fmt.Errorf("check %q via master: %w", res.Name, err)
	}
	if !exists {
		if _, err := master.ExecContext(ctx, "CREATE DATABASE "+msQuoteIdent(res.Name)); err != nil {
			// 1801: another process created it between the check and the
			// create — the goal is met either way, and that process lets the
			// login in.
			if isSQLServerError(err, 1801) {
				return res, nil
			}
			return res, fmt.Errorf("create database %q: %w", res.Name, err)
		}
		res.DatabaseCreated = true
	}

	// Decided on what is there, not on what this call created: a call that was
	// interrupted after CREATE DATABASE leaves a database its login cannot
	// open, and the next call has to finish the job. A login that already has
	// a user in the database is left as its owner set it up.
	if provision {
		maint.Database = res.Name
		target := sql.OpenDB(mssql.NewConnectorConfig(maint))
		defer target.Close()
		user := msQuoteIdent(cfg.User)
		if _, err := target.ExecContext(ctx,
			"SET XACT_ABORT ON; "+
				"IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE sid = SUSER_SID("+
				msQuoteLiteral(cfg.User)+")) "+
				"BEGIN BEGIN TRANSACTION; CREATE USER "+user+" FOR LOGIN "+user+
				"; ALTER ROLE db_owner ADD MEMBER "+user+"; COMMIT; END;",
		); err != nil {
			return res, fmt.Errorf("give login %q the database %q: %w", cfg.User, res.Name, err)
		}
	}
	return res, nil
}

func (sqlServerDriver) Preflight(ctx context.Context, dsn string, db *sql.DB) framing.PreflightReport {
	report := framing.PreflightReport{Dialect: "sqlserver"}
	if cfg, err := sqlServerTarget(dsn); err == nil {
		report.User, report.Database = cfg.User, cfg.Database
	}
	login, createDB := msCreateDB(ctx, db, &report.Details)
	report.CreateDB = createDB
	if report.User == "" {
		report.User = login // integrated authentication: the DSN names nobody
	}
	// CREATE TABLE IF NOT EXISTS is not T-SQL (error 156).
	report.DDL, report.DML = probeDDLDML(ctx, db,
		"IF OBJECT_ID(N'"+probeTable+"', N'U') IS NULL CREATE TABLE "+probeTable+" (id int)",
		&report.Details)

	var major, edition, level int
	if err := db.QueryRowContext(ctx,
		"SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS INT), "+
			"CAST(SERVERPROPERTY('EngineEdition') AS INT), compatibility_level "+
			"FROM sys.databases WHERE name = DB_NAME()",
	).Scan(&major, &edition, &level); err != nil {
		report.Details = append(report.Details, fmt.Sprintf("version check failed: %v", err))
	} else if problem := sqlServerVersionProblem(major, edition, level); problem != "" {
		// The rendered SQL would not run, whatever the rights are.
		no := false
		report.DDL = &no
		report.Details = append(report.Details, problem)
	}
	return report
}

// SERVERPROPERTY('EngineEdition') of the managed services. They run the current
// engine and still report ProductMajorVersion 12.
const (
	engineAzureSQLDatabase        = 5
	engineAzureSQLManagedInstance = 8
)

// sqlServerVersionProblem says why the rendered SQL cannot run on this
// database, or "" when it can: the scripts use SQL Server 2022 syntax (major
// version 16, or an Azure service: IS DISTINCT FROM, JSON_OBJECT, ISJSON(…,
// VALUE)) and OPENJSON, which needs compatibility level 130 everywhere.
func sqlServerVersionProblem(major, edition, level int) string {
	current := major >= 16 ||
		edition == engineAzureSQLDatabase || edition == engineAzureSQLManagedInstance
	if current && level >= 130 {
		return ""
	}
	return fmt.Sprintf("SQL Server 2022 and compatibility level 130 or later are required "+
		"(this database: major version %d, compatibility level %d)", major, level)
}

func (sqlServerDriver) HostPreflight(ctx context.Context, baseDSN, adminDSN string) framing.PreflightReport {
	report := framing.PreflightReport{Dialect: "sqlserver"}
	dsn, label := provisioningDSN(baseDSN, adminDSN, &report.Details)
	cfg, err := sqlServerConfig(dsn, msServerForm)
	if err != nil {
		report.Details = append(report.Details, fmt.Sprintf("%s: %v", label, err))
		return report
	}
	report.User = cfg.User
	if cfg.Database == "" {
		cfg.Database = "master"
	}
	db := sql.OpenDB(mssql.NewConnectorConfig(cfg))
	defer db.Close()
	login, createDB := msCreateDB(ctx, db, &report.Details)
	report.CreateDB = createDB
	if report.User == "" {
		report.User = login
	}
	if createDB != nil && !*createDB {
		// A server-level right can only be granted from master (error 4621).
		report.Details = append(report.Details,
			"grant the create-database right with: USE master; GRANT CREATE ANY DATABASE TO "+
				msQuoteIdent(login))
	}
	return report
}

// Snapshot runs the script's GO-separated batches in order on one connection:
// the session settings, the transaction the first batch opens and the step
// counter the batches check all live in that session. It stops at the first
// batch the server refuses; the connection is then discarded, which rolls back
// whatever the transaction held.
func (sqlServerDriver) Snapshot(ctx context.Context, db *sql.DB, script string) error {
	batches := splitBatches(script)
	return onDedicatedConn(ctx, db, func(conn *sql.Conn) error {
		for i, batch := range batches {
			if _, err := conn.ExecContext(ctx, batch.sql); err != nil {
				return &BatchError{Index: i, StartLine: batch.startLine, SQL: batch.sql, Err: err}
			}
		}
		return nil
	})
}

// BatchError is a snapshot batch the server refused. SQL Server numbers the
// lines of an error from the start of its batch, so the batch travels with
// the error for DescribeError to name the line of the whole script.
type BatchError struct {
	Index     int    // position among the batches that ran, from 0
	StartLine int    // script line the batch starts on, from 1
	SQL       string // the batch as it was sent
	Err       error
}

func (e *BatchError) Error() string { return fmt.Sprintf("batch %d: %v", e.Index+1, e.Err) }
func (e *BatchError) Unwrap() error { return e.Err }

type sqlBatch struct {
	startLine int
	sql       string
}

// isGoLine reports a batch separator: a line holding nothing but GO. It is a
// convention of the SQL Server client tools, not T-SQL — the server would
// refuse the word — so whoever sends a script cuts it there. The server side
// never renders a literal with such a line.
func isGoLine(line string) bool {
	line = strings.Trim(strings.TrimSuffix(line, "\r"), " \t")
	return strings.EqualFold(line, "GO")
}

// splitBatches cuts a script on its GO lines and drops the batches that hold
// nothing. A batch keeps its leading lines untouched: the server counts error
// lines from its first one.
func splitBatches(script string) []sqlBatch {
	var out []sqlBatch
	batchStart, batchLine := 0, 1
	flush := func(end int) {
		if text := script[batchStart:end]; strings.TrimSpace(text) != "" {
			out = append(out, sqlBatch{startLine: batchLine, sql: strings.TrimRight(text, " \t\r\n")})
		}
	}
	line := 1
	for lineStart := 0; lineStart < len(script); line++ {
		next := len(script)
		lineEnd := next
		if nl := strings.IndexByte(script[lineStart:], '\n'); nl >= 0 {
			lineEnd = lineStart + nl
			next = lineEnd + 1
		}
		if isGoLine(script[lineStart:lineEnd]) {
			flush(lineStart)
			batchStart, batchLine = next, line+1
		}
		lineStart = next
	}
	flush(len(script))
	return out
}

// DescribeError adds what Error() leaves out of a SQL Server error — its
// state — spells its number as a field, and names the offending line of the
// script when the server's line number can be trusted. With several errors
// for one statement the first is described: it is the cause, the later ones
// say what failed because of it.
func (sqlServerDriver) DescribeError(script string, err error) string {
	var dbErr mssql.Error
	if !errors.As(err, &dbErr) {
		return err.Error()
	}
	if len(dbErr.All) > 0 {
		dbErr = dbErr.All[0]
	}
	batch, startLine := script, 1 // a delta is sent whole, as one batch
	var batchErr *BatchError
	if errors.As(err, &batchErr) {
		batch, startLine = batchErr.SQL, batchErr.StartLine
	}
	var b strings.Builder
	b.WriteString(err.Error())
	describeField(&b, "number", strconv.Itoa(int(dbErr.Number)))
	describeField(&b, "state", strconv.Itoa(int(dbErr.State)))
	if line, text, ok := locateInBatch(batch, dbErr); ok {
		describeField(&b, fmt.Sprintf("script line %d", startLine+line-1), text)
	}
	return b.String()
}

// dynamicSQL finds a statement that runs a string as its own batch. The
// server numbers an error raised in there from the first line of that string.
var dynamicSQL = regexp.MustCompile(`(?i)\bEXEC(UTE)?\s*\(|\bsp_executesql\b`)

// inertLine matches a line that cannot raise: blank, a comment, or session
// settings (`SET XACT_ABORT ON; SET QUOTED_IDENTIFIER ON;`).
var inertLine = regexp.MustCompile(`(?i)^\s*(--.*|(SET\s+\w+\s+(ON|OFF)\s*;?\s*)*)$`)

// locateInBatch maps the line number of a SQL Server error to a line of the
// batch that raised it (from 1), or reports that the number names no line of
// the batch.
//
// The number counts from the start of the innermost batch. A migration
// statement is rendered as EXEC(N'…') on one line, so an error raised inside
// one says "line 1" wherever the EXEC stands. Line 1 is therefore reported
// only when a single line of the batch can have raised it; any other number
// is a line of the batch itself.
func locateInBatch(batch string, dbErr mssql.Error) (int, string, bool) {
	if dbErr.ProcName != "" {
		return 0, "", false // a line of that procedure
	}
	lines := strings.Split(batch, "\n")
	line := int(dbErr.LineNo)
	if line < 1 || line > len(lines) {
		return 0, "", false
	}
	if line == 1 {
		var candidates []int
		for i, text := range lines {
			if dynamicSQL.MatchString(text) || (i == 0 && !inertLine.MatchString(text)) {
				candidates = append(candidates, i+1)
			}
		}
		if len(candidates) != 1 {
			return 0, "", false
		}
		line = candidates[0]
	}
	return line, strings.TrimSpace(lines[line-1]), true
}

// msCreateDB reads the connected login and its server-level right to create
// databases. CREATE DATABASE is non-transactional, so this reads the
// permission rather than probing.
func msCreateDB(ctx context.Context, db *sql.DB, details *[]string) (string, *bool) {
	var login sql.NullString
	var allowed sql.NullInt64
	if err := db.QueryRowContext(ctx,
		"SELECT SUSER_SNAME(), HAS_PERMS_BY_NAME(NULL, NULL, 'CREATE ANY DATABASE')",
	).Scan(&login, &allowed); err != nil {
		*details = append(*details, fmt.Sprintf("create-database check failed: %v", err))
		return "", nil
	}
	createDB := allowed.Int64 == 1
	return login.String, &createDB
}

func isSQLServerError(err error, number int32) bool {
	var dbErr mssql.Error
	return errors.As(err, &dbErr) && dbErr.Number == number
}

func msQuoteIdent(s string) string {
	return "[" + strings.ReplaceAll(s, "]", "]]") + "]"
}

// msQuoteLiteral quotes a string literal for statements that cannot take bind
// parameters (CREATE LOGIN ... WITH PASSWORD): double every single quote.
func msQuoteLiteral(s string) string {
	return "N'" + strings.ReplaceAll(s, "'", "''") + "'"
}

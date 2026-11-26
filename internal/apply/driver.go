package apply

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/TOT-Concept/ee-database/internal/framing"
)

// Driver is everything that differs between target database types. A
// registration's dialect picks one (For); the rest of the client — pairing,
// the delta loop, Batch, the apply log — is written once against it.
type Driver interface {
	// Dialect is the wire name the server uses: "postgres" | "sqlserver".
	Dialect() string
	// Label is the product name shown to the operator: "PostgreSQL" |
	// "SQL Server".
	Label() string
	// DefaultPort is the server port the database type listens on by default.
	DefaultPort() int
	// ComposeDSN writes the target DSN of these connection parameters, the
	// user and password escaped, so `run` can ask for the parts instead of
	// the URL.
	ComposeDSN(p DSNParts) string
	// connectProblem reads the error codes the server reports when it
	// refuses a connection (DiagnoseConnect reads the network failures).
	connectProblem(err error) ConnectProblem
	// Open returns a pool whose Exec accepts a delta's multi-statement script.
	Open(dsn string) (*sql.DB, error)
	// DeriveDSN composes a per-database DSN from a host's base DSN (a server,
	// no database name) and the physical database name. The base DSN never
	// leaves the machine; the name is the server-suggested snake_cased
	// registration name unless overridden in the host config.
	DeriveDSN(baseDSN, databaseName string) (string, error)
	// EnsureDatabase creates the database named in the DSN when it does not
	// exist yet (`run --create-missing`, and always in managed mode). Without
	// adminDSN the DSN's own credentials are used, so they need the
	// server-level create right. With adminDSN the admin connection bootstraps
	// everything the target DSN names: the login (with the target DSN's
	// password) when it does not exist, then the database owned by it.
	EnsureDatabase(ctx context.Context, dsn, adminDSN string) (EnsureResult, error)
	// Preflight self-checks the target connection's provisioning rights and
	// reports them (nil = not determinable):
	//
	//   - create_db — server-level right to create databases, read from the
	//     catalog rather than probed.
	//   - ddl / dml — probe table in the default schema: CREATE proves DDL,
	//     INSERT+DELETE prove DML, DROP cleans up. Writing the replica is this
	//     client's whole mandate, so the probe is within it.
	//
	// The check is advisory — grants can change afterwards; apply errors
	// remain the safety net. The report is shown on the Sync client card, so a
	// gap is visible before the user publishes and waits on a delta that
	// cannot apply.
	Preflight(ctx context.Context, dsn string, db *sql.DB) framing.PreflightReport
	// HostPreflight is the pair-time / managed-run-startup check of the host's
	// provisioning identity, at SERVER level: the create-database right of the
	// effective creating login (the admin DSN when given, else the base DSN's
	// own), through a maintenance connection. A missing right adds the
	// statement that grants it to the report's details. DDL/DML stay nil here —
	// a database the host creates is owned by the run login (ownership implies
	// both), and pre-existing targets are probed per database at claim time.
	HostPreflight(ctx context.Context, baseDSN, adminDSN string) framing.PreflightReport
	// Snapshot applies the bootstrap script. The script opens and commits its
	// own transaction, and every statement is idempotent, so applying it onto
	// a non-empty replica converges. A failure applies nothing.
	Snapshot(ctx context.Context, db *sql.DB, script string) error
	// DescribeError renders everything the database reported about a failed
	// script: the driver's one-line Error() plus the fields it drops and, when
	// the server located the error, the offending line of the script.
	DescribeError(script string, err error) string
}

// DSNParts are the connection parameters of a target database.
type DSNParts struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

func (p DSNParts) userinfo() *url.Userinfo { return url.UserPassword(p.User, p.Password) }
func (p DSNParts) hostPort() string        { return net.JoinHostPort(p.Host, strconv.Itoa(p.Port)) }

// EnsureResult reports what EnsureDatabase actually created.
type EnsureResult struct {
	DatabaseCreated bool
	LoginCreated    bool
	Name            string
}

// For resolves a registration's dialect to its driver. An empty dialect is a
// profile paired from a pasted token before the server reported database
// metadata: PostgreSQL was the only target then.
func For(dialect string) (Driver, error) {
	switch dialect {
	case "", "postgres":
		return postgresDriver{}, nil
	case "sqlserver":
		return sqlServerDriver{}, nil
	default:
		return nil, fmt.Errorf("unsupported dialect %q", dialect)
	}
}

// ForDSN is the driver of the database type a connection URL names by its
// scheme: postgres:// or postgresql://, sqlserver://.
func ForDSN(dsn string) (Driver, error) {
	scheme, _, found := strings.Cut(strings.TrimSpace(dsn), "://")
	if !found {
		return nil, errors.New("not a URL: postgres://user:pass@host:5432/ or sqlserver://user:pass@host:1433")
	}
	switch strings.ToLower(scheme) {
	case "postgres", "postgresql":
		return postgresDriver{}, nil
	case "sqlserver":
		return sqlServerDriver{}, nil
	default:
		return nil, fmt.Errorf("unsupported database type %q: postgres:// or sqlserver://", scheme)
	}
}

// onDedicatedConn runs a snapshot's statements on one connection that is
// discarded on failure: a failed multi-statement script can leave the session
// inside an aborted transaction (pgx < v5.8 pooled such connections, so every
// retry reported SQLSTATE 25P02 instead of the root cause), and no driver is
// trusted to clean that up.
func onDedicatedConn(ctx context.Context, db *sql.DB, run func(*sql.Conn) error) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	runErr := run(conn)
	if runErr != nil {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
	if closeErr := conn.Close(); runErr == nil {
		return closeErr
	}
	return runErr
}

// describeField appends one `label: value` line to a described error. The
// labels are a wire format: the Database Sync page lays the fields out.
func describeField(b *strings.Builder, label, value string) {
	if value != "" {
		fmt.Fprintf(b, "\n  %s: %s", label, value)
	}
}

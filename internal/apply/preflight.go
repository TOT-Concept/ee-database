package apply

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/TOT-Concept/ee-database/internal/framing"
)

// probeTable is created, written and dropped by the DDL/DML probe. A real
// table (not TEMP) because TEMP proves a different privilege than the deltas
// need, and a probe is the only check portable across all three dialects.
const probeTable = "_ee_preflight"

// Preflight self-checks the target connection's provisioning rights and
// reports them (nil = not determinable on this dialect):
//
//   - create_db — server-level right to create databases (postgres: CREATEDB
//     or superuser via pg_roles; mysql: global CREATE; sqlite: always true,
//     the driver creates the file). CREATE DATABASE is non-transactional on
//     postgres, so this reads the catalog rather than probing.
//   - ddl / dml — probe table in the default schema: CREATE proves DDL,
//     INSERT+DELETE prove DML, DROP cleans up. Writing the replica is this
//     client's whole mandate, so the probe is within it.
//
// The check is advisory — grants can change afterwards; apply errors remain
// the safety net. The report is shown on the Sync client card, so a gap is
// visible before the user publishes and waits on a delta that cannot apply.
func Preflight(ctx context.Context, dialect, dsn string, db *sql.DB) framing.PreflightReport {
	report := framing.PreflightReport{Dialect: dialect}
	if report.Dialect == "" {
		report.Dialect = "postgres"
	}

	switch report.Dialect {
	case "postgres":
		if cfg, err := pgx.ParseConfig(dsn); err == nil {
			report.User, report.Database = cfg.User, cfg.Database
		}
		var createDB bool
		err := db.QueryRowContext(ctx,
			"SELECT rolcreatedb OR rolsuper FROM pg_roles WHERE rolname = current_user",
		).Scan(&createDB)
		if err != nil {
			report.Details = append(report.Details, fmt.Sprintf("CREATEDB check failed: %v", err))
		} else {
			report.CreateDB = &createDB
		}
	case "mysql":
		if cfg, err := gomysql.ParseDSN(dsn); err == nil {
			report.User, report.Database = cfg.User, cfg.DBName
		}
		var createDB bool
		err := db.QueryRowContext(ctx,
			`SELECT COUNT(*) > 0 FROM information_schema.user_privileges
			 WHERE privilege_type IN ('CREATE', 'SUPER')
			   AND grantee = CONCAT("'", SUBSTRING_INDEX(CURRENT_USER(), '@', 1),
			                        "'@'", SUBSTRING_INDEX(CURRENT_USER(), '@', -1), "'")`,
		).Scan(&createDB)
		if err != nil {
			report.Details = append(report.Details, fmt.Sprintf("CREATE privilege check failed: %v", err))
		} else {
			report.CreateDB = &createDB
		}
	case "sqlite":
		report.Database = dsn
		yes := true
		report.CreateDB = &yes
		report.Details = append(report.Details, "sqlite creates the database file on open")
	default:
		report.Details = append(report.Details, fmt.Sprintf("unsupported dialect %q", dialect))
		return report
	}

	report.DDL, report.DML = probeDDLDML(ctx, db, &report.Details)
	return report
}

// HostPreflight is the pair-time / managed-run-startup check of the host's
// provisioning identity, at SERVER level: CREATEDB for the effective creating
// user (the admin DSN when given, else the base DSN's own user), through a
// maintenance connection. DDL/DML stay nil here — a database the host creates
// is owned by the run user (ownership implies both), and pre-existing targets
// are probed per database at claim time.
func HostPreflight(ctx context.Context, dialect, baseDSN, adminDSN string) framing.PreflightReport {
	report := framing.PreflightReport{Dialect: dialect}
	if report.Dialect == "" {
		report.Dialect = "postgres"
	}
	effective := baseDSN
	if adminDSN != "" {
		effective = adminDSN
		report.Details = append(report.Details, "database creation uses --admin-dsn")
	}

	switch report.Dialect {
	case "postgres":
		cfg, err := pgx.ParseConfig(effective)
		if err != nil {
			report.Details = append(report.Details, fmt.Sprintf("base dsn: %v", err))
			return report
		}
		report.User = cfg.User
		if cfg.Database == "" {
			cfg.Database = "postgres"
		}
		db := sql.OpenDB(stdlib.GetConnector(*cfg))
		defer db.Close()
		var createDB bool
		err = db.QueryRowContext(ctx,
			"SELECT rolcreatedb OR rolsuper FROM pg_roles WHERE rolname = current_user",
		).Scan(&createDB)
		if err != nil {
			report.Details = append(report.Details, fmt.Sprintf("CREATEDB check failed: %v", err))
		} else {
			report.CreateDB = &createDB
		}
	case "mysql":
		cfg, err := gomysql.ParseDSN(effective)
		if err != nil {
			report.Details = append(report.Details, fmt.Sprintf("base dsn: %v", err))
			return report
		}
		report.User = cfg.User
		maint := cfg.Clone()
		maint.DBName = ""
		connector, err := gomysql.NewConnector(maint)
		if err != nil {
			report.Details = append(report.Details, fmt.Sprintf("mysql: %v", err))
			return report
		}
		db := sql.OpenDB(connector)
		defer db.Close()
		var createDB bool
		err = db.QueryRowContext(ctx,
			`SELECT COUNT(*) > 0 FROM information_schema.user_privileges
			 WHERE privilege_type IN ('CREATE', 'SUPER')
			   AND grantee = CONCAT("'", SUBSTRING_INDEX(CURRENT_USER(), '@', 1),
			                        "'@'", SUBSTRING_INDEX(CURRENT_USER(), '@', -1), "'")`,
		).Scan(&createDB)
		if err != nil {
			report.Details = append(report.Details, fmt.Sprintf("CREATE privilege check failed: %v", err))
		} else {
			report.CreateDB = &createDB
		}
	case "sqlite":
		probe := filepath.Join(baseDSN, ".ee-preflight")
		writable := os.WriteFile(probe, nil, 0o600) == nil
		_ = os.Remove(probe)
		report.CreateDB = &writable
		if !writable {
			report.Details = append(report.Details,
				fmt.Sprintf("directory %s is not writable", baseDSN))
		}
	default:
		report.Details = append(report.Details, fmt.Sprintf("unsupported dialect %q", dialect))
	}
	return report
}

func probeDDLDML(ctx context.Context, db *sql.DB, details *[]string) (ddl, dml *bool) {
	no, yes := false, true
	if _, err := db.ExecContext(ctx,
		"CREATE TABLE IF NOT EXISTS "+probeTable+" (id integer)"); err != nil {
		*details = append(*details, fmt.Sprintf("DDL probe (CREATE TABLE) failed: %v", err))
		return &no, nil // cannot tell DML apart without a table to write
	}
	ddl = &yes

	dml = &yes
	if _, err := db.ExecContext(ctx,
		"INSERT INTO "+probeTable+" (id) VALUES (1)"); err != nil {
		*details = append(*details, fmt.Sprintf("DML probe (INSERT) failed: %v", err))
		dml = &no
	} else if _, err := db.ExecContext(ctx, "DELETE FROM "+probeTable); err != nil {
		*details = append(*details, fmt.Sprintf("DML probe (DELETE) failed: %v", err))
		dml = &no
	}

	if _, err := db.ExecContext(ctx, "DROP TABLE "+probeTable); err != nil {
		*details = append(*details, fmt.Sprintf("probe cleanup (DROP TABLE) failed: %v", err))
	}
	return ddl, dml
}

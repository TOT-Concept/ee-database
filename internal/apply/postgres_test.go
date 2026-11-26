package apply

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestPostgresDeriveDSN(t *testing.T) {
	pg := postgresDriver{}
	got, err := pg.DeriveDSN("postgres://ee_replica:pw@localhost:15438/", "vinyl_db")
	if err != nil || got != "postgres://ee_replica:pw@localhost:15438/vinyl_db" {
		t.Errorf("got %q err=%v", got, err)
	}
	// Query params (sslmode) must survive the path swap.
	got, err = pg.DeriveDSN("postgres://u:p@h:5432/?sslmode=require", "my_db")
	if err != nil || got != "postgres://u:p@h:5432/my_db?sslmode=require" {
		t.Errorf("with params: got %q err=%v", got, err)
	}
}

func TestPostgresDeriveDSNRejects(t *testing.T) {
	pg := postgresDriver{}
	if _, err := pg.DeriveDSN("postgres://u@h/", ""); err == nil {
		t.Error("want error for empty database name")
	}
	// Another database type's URL is not a PostgreSQL server either.
	for _, base := range []string{
		"not a url", "host=h user=u password=secret", "sqlserver://u:secret@h:1433", "postgres://u:secret@h:port/",
	} {
		_, err := pg.DeriveDSN(base, "db")
		if err == nil {
			t.Errorf("want error for base dsn %q", base)
		} else if strings.Contains(err.Error(), "secret") {
			t.Errorf("the error quotes the password: %v", err)
		}
	}
	if got, err := pg.DeriveDSN("postgresql://u:p@h/", "db"); err != nil || got != "postgresql://u:p@h/db" {
		t.Errorf("the postgresql:// spelling: got %q err=%v", got, err)
	}
}

func TestPostgresQuoting(t *testing.T) {
	if got := pgQuoteIdent(`video_games`); got != `"video_games"` {
		t.Errorf("pgQuoteIdent = %s", got)
	}
	if got := pgQuoteIdent(`we"ird`); got != `"we""ird"` {
		t.Errorf("pgQuoteIdent escaping = %s", got)
	}
	if got := pgQuoteLiteral("pa'ss"); got != "'pa''ss'" {
		t.Errorf("pgQuoteLiteral = %s", got)
	}
}

func TestPostgresOpenRejectsABadDSN(t *testing.T) {
	_, err := postgresDriver{}.Open("postgres://\x00bad")
	if err == nil || !strings.HasPrefix(err.Error(), "postgres dsn: ") {
		t.Errorf("err = %v, want a postgres dsn parse error", err)
	}
}

// A sync moved to another database type keeps its old DSN in the operator's
// shell history: pgx would read it as no setting and open the local server.
func TestPostgresRefusesTheDSNOfAnotherDatabaseType(t *testing.T) {
	for _, dsn := range []string{
		"sqlserver://u:secret@h:1433?database=d", "mysql://u:secret@h/d",
	} {
		if _, err := (postgresDriver{}).Open(dsn); err == nil ||
			!strings.Contains(err.Error(), "not a PostgreSQL one") ||
			strings.Contains(err.Error(), "secret") {
			t.Errorf("%s: err = %v", dsn, err)
		}
		if _, err := (postgresDriver{}).EnsureDatabase(context.Background(), dsn, ""); err == nil {
			t.Errorf("%s: EnsureDatabase accepted it", dsn)
		}
	}
	for _, dsn := range []string{
		"postgres://u:p@h/d", "postgresql://u:p@h/d",
		"host=h user=u dbname=d password=a://b",
	} {
		db, err := (postgresDriver{}).Open(dsn)
		if err != nil {
			t.Errorf("%s: %v", dsn, err)
			continue
		}
		_ = db.Close()
	}
}

func TestPostgresEnsureDatabaseBadDSN(t *testing.T) {
	_, err := postgresDriver{}.EnsureDatabase(context.Background(), "postgres://\x00bad", "")
	if err == nil {
		t.Error("want parse error for invalid postgres dsn")
	}
}

func TestPostgresEnsureDatabaseBadAdminDSN(t *testing.T) {
	_, err := postgresDriver{}.EnsureDatabase(context.Background(),
		"postgres://ee_replica:pw@localhost:5432/replica_db", "postgres://\x00bad")
	if err == nil || !strings.Contains(err.Error(), "admin dsn") {
		t.Errorf("err = %v, want admin dsn parse error", err)
	}
}

func TestPostgresHostPreflightBadDSN(t *testing.T) {
	report := postgresDriver{}.HostPreflight(context.Background(), "postgres://\x00bad", "")
	if report.Dialect != "postgres" || report.CreateDB != nil {
		t.Errorf("an unreadable base dsn must determine nothing: %+v", report)
	}
	if len(report.Details) != 1 || !strings.HasPrefix(report.Details[0], "base dsn: ") {
		t.Errorf("details = %q", report.Details)
	}
}

func TestPostgresDescribeErrorFields(t *testing.T) {
	err := &pgconn.PgError{
		Severity: "ERROR", Code: "42701",
		Message: `column "very_long_name_" specified more than once`,
		Detail:  "the detail", Hint: "the hint", TableName: "magic_mushroom",
	}
	got := postgresDriver{}.DescribeError("CREATE TABLE x ();", err)
	for _, want := range []string{
		"SQLSTATE 42701", "detail: the detail", "hint: the hint", "table: magic_mushroom",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	if strings.Contains(got, "script line") {
		t.Errorf("position 0 must not render a script line:\n%s", got)
	}
}

func TestPostgresDescribeErrorPosition(t *testing.T) {
	script := "BEGIN;\nCREATE TABLE t (\n    \"a\" nosuchtype\n);\nCOMMIT;\n"
	pos := strings.Index(script, "nosuchtype") + 1 // ASCII: bytes == characters
	wrapped := fmt.Errorf("delta 430 (schema migrate): %w", &pgconn.PgError{
		Severity: "ERROR", Code: "42704",
		Message: `type "nosuchtype" does not exist`, Position: int32(pos),
	})
	got := postgresDriver{}.DescribeError(script, wrapped)
	if !strings.HasPrefix(got, "delta 430 (schema migrate): ERROR:") {
		t.Errorf("wrap prefix lost:\n%s", got)
	}
	if !strings.Contains(got, `script line 3: "a" nosuchtype`) {
		t.Errorf("missing located line in:\n%s", got)
	}
}

// An error the database did not send (a dropped connection, a context
// deadline) has no fields to add.
func TestPostgresDescribeErrorPassesOtherErrorsThrough(t *testing.T) {
	err := errors.New("delta 7 (data upsert): unexpected EOF")
	if got := (postgresDriver{}).DescribeError("SELECT 1;", err); got != err.Error() {
		t.Errorf("a non-database error must pass through, got:\n%s", got)
	}
}

func TestLocateInScriptBounds(t *testing.T) {
	line, text := locateInScript("SELECT 1;\nSELECT 2;", 1000)
	if line != 2 || text != "SELECT 2;" {
		t.Errorf("out-of-range cursor should land on the last line, got %d %q", line, text)
	}
	// The located line is reported whole — it becomes the quarantine reason
	// shown (and copied) in the Database Sync page, where a truncated
	// statement is useless.
	long := "SELECT '" + strings.Repeat("é", 300) + "';"
	_, text = locateInScript(long, 3)
	if text != long {
		t.Errorf("long line must be reported untruncated, got %d runes", len([]rune(text)))
	}
}

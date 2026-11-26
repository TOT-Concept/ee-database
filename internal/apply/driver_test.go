package apply

import (
	"context"
	"slices"
	"strings"
	"testing"
)

func TestForResolvesPostgres(t *testing.T) {
	// "" is a profile paired before the server reported the dialect.
	for _, dialect := range []string{"", "postgres"} {
		driver, err := For(dialect)
		if err != nil {
			t.Fatalf("For(%q): %v", dialect, err)
		}
		if driver.Dialect() != "postgres" || driver.Label() != "PostgreSQL" {
			t.Errorf("For(%q) = %s / %s", dialect, driver.Dialect(), driver.Label())
		}
	}
}

func TestForUnsupportedDialect(t *testing.T) {
	for _, dialect := range []string{"oracle", "mysql", "sqlite", "Postgres"} {
		driver, err := For(dialect)
		if err == nil || driver != nil {
			t.Errorf("For(%q) = %v, %v — want an error and no driver", dialect, driver, err)
			continue
		}
		if want := `unsupported dialect "` + dialect + `"`; err.Error() != want {
			t.Errorf("For(%q) error = %q, want %q", dialect, err, want)
		}
	}
}

func TestSnapshotKeepsTheConnectionOnSuccess(t *testing.T) {
	db, fake := openFake(t)
	script := "BEGIN;\nCREATE TABLE t (k text);\nCOMMIT;"
	if err := (postgresDriver{}).Snapshot(context.Background(), db, script); err != nil {
		t.Fatal(err)
	}
	// One Exec for the whole script; the connection goes back to the pool.
	if got := fake.sent(); !slices.Equal(got, []string{script}) {
		t.Fatalf("sent to the database: %q", got)
	}
}

// A failed script can leave the session inside an aborted transaction: the
// connection is closed instead of pooled, so the retry starts clean.
func TestSnapshotDiscardsTheConnectionOnFailure(t *testing.T) {
	db, fake := openFake(t)
	fake.failOn = "nosuchtype"
	script := "BEGIN;\nCREATE TABLE t (k nosuchtype);\nCOMMIT;"
	err := (postgresDriver{}).Snapshot(context.Background(), db, script)
	if err == nil || !strings.Contains(err.Error(), "statement refused") {
		t.Fatalf("err = %v, want the database's refusal", err)
	}
	if got := fake.sent(); !slices.Equal(got, []string{script, "CLOSE"}) {
		t.Fatalf("sent to the database: %q", got)
	}
}

func TestProbeDDLDML(t *testing.T) {
	const create = "CREATE TABLE IF NOT EXISTS " + probeTable + " (id integer)"
	insert := "INSERT INTO " + probeTable + " (id) VALUES (1)"
	remove := "DELETE FROM " + probeTable
	drop := "DROP TABLE " + probeTable

	cases := []struct {
		name     string
		failOn   string
		ddl, dml *bool
		sent     []string
		detail   string
	}{
		{name: "all rights", ddl: ptr(true), dml: ptr(true),
			sent: []string{create, insert, remove, drop}},
		{name: "no DDL", failOn: "CREATE TABLE", ddl: ptr(false), dml: nil,
			sent: []string{create}, detail: "DDL probe (CREATE TABLE) failed"},
		// The probe table is dropped even when writing it was refused.
		{name: "no INSERT", failOn: "INSERT", ddl: ptr(true), dml: ptr(false),
			sent: []string{create, insert, drop}, detail: "DML probe (INSERT) failed"},
		{name: "no DELETE", failOn: "DELETE", ddl: ptr(true), dml: ptr(false),
			sent: []string{create, insert, remove, drop}, detail: "DML probe (DELETE) failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, fake := openFake(t)
			fake.failOn = tc.failOn
			var details []string
			ddl, dml := probeDDLDML(context.Background(), db, create, &details)
			if !sameBool(ddl, tc.ddl) || !sameBool(dml, tc.dml) {
				t.Errorf("ddl = %s, dml = %s", show(ddl), show(dml))
			}
			if got := fake.sent(); !slices.Equal(got, tc.sent) {
				t.Errorf("sent to the database: %q", got)
			}
			if joined := strings.Join(details, "; "); !strings.Contains(joined, tc.detail) ||
				(tc.detail == "" && joined != "") {
				t.Errorf("details = %q", joined)
			}
		})
	}
}

func TestProvisioningDSN(t *testing.T) {
	var details []string
	if got, label := provisioningDSN("base", "", &details); got != "base" || label != "base dsn" || len(details) != 0 {
		t.Errorf("without an admin DSN: %q (%s), details %q", got, label, details)
	}
	if got, label := provisioningDSN("base", "admin", &details); got != "admin" || label != "admin dsn" ||
		!slices.Equal(details, []string{"database creation uses --admin-dsn"}) {
		t.Errorf("with an admin DSN: %q (%s), details %q", got, label, details)
	}
}

func ptr(v bool) *bool { return &v }

func sameBool(a, b *bool) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func show(v *bool) string {
	switch {
	case v == nil:
		return "nil"
	case *v:
		return "true"
	default:
		return "false"
	}
}

func TestForDSNReadsTheScheme(t *testing.T) {
	for dsn, want := range map[string]string{
		"postgres://u:p@h:5432/":    "postgres",
		"postgresql://u:p@h/db":     "postgres",
		" SQLServer://sa:p@h:1433 ": "sqlserver",
	} {
		driver, err := ForDSN(dsn)
		if err != nil || driver.Dialect() != want {
			t.Errorf("ForDSN(%q) = %v, %v; want %s", dsn, driver, err, want)
		}
	}
	for _, dsn := range []string{"mysql://u:p@h/db", "host=h user=u", ""} {
		if _, err := ForDSN(dsn); err == nil {
			t.Errorf("ForDSN(%q) accepted", dsn)
		}
	}
}

// A DSN composed from prompted parameters reads back, through the parser the
// driver opens it with, as those same parameters — a password full of URL
// delimiters included.
func TestComposeDSNRoundTrips(t *testing.T) {
	parts := DSNParts{Host: "db.local", Port: 15433, User: "ee_replica",
		Password: "p@ss:w/rd?#&=%", Database: "scn001_chess"}

	pg, err := postgresConfig(postgresDriver{}.ComposeDSN(parts))
	if err != nil {
		t.Fatalf("postgres: %v", err)
	}
	if pg.Host != parts.Host || int(pg.Port) != parts.Port || pg.User != parts.User ||
		pg.Password != parts.Password || pg.Database != parts.Database {
		t.Errorf("postgres round trip = %s:%d %s/%s db %s", pg.Host, pg.Port, pg.User, pg.Password, pg.Database)
	}

	ms, err := sqlServerTarget(sqlServerDriver{}.ComposeDSN(parts))
	if err != nil {
		t.Fatalf("sqlserver: %v", err)
	}
	if ms.Host != parts.Host || int(ms.Port) != parts.Port || ms.User != parts.User ||
		ms.Password != parts.Password || ms.Database != parts.Database {
		t.Errorf("sqlserver round trip = %s:%d %s/%s db %s", ms.Host, ms.Port, ms.User, ms.Password, ms.Database)
	}
}

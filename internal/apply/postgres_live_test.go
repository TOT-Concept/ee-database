package apply

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/TOT-Concept/ee-database/internal/framing"
)

// The live tests run the driver against a real server. They need a superuser
// DSN in EE_TEST_POSTGRES_DSN (the variable the backend's conformance suite
// reads too) and skip without it: the public CI has no database. Everything
// they create is named ee_t222_go_<random> and dropped at the end.

func liveName(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 4)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	return "ee_t222_go_" + hex.EncodeToString(raw)
}

func TestPostgresLive(t *testing.T) {
	adminDSN := os.Getenv("EE_TEST_POSTGRES_DSN")
	if adminDSN == "" {
		t.Skip("EE_TEST_POSTGRES_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pg, err := For("postgres")
	if err != nil {
		t.Fatal(err)
	}

	// A login that does not exist yet, on the admin DSN's server: the base
	// DSN of a managed host, and the target DSN `--admin-dsn` provisions.
	name := liveName(t)
	base, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	base.User = url.UserPassword(name, "it's a \"secret\"")
	base.Path = "/"
	baseDSN := base.String()
	targetDSN, err := pg.DeriveDSN(baseDSN, name)
	if err != nil {
		t.Fatal(err)
	}
	ownDSN, err := pg.DeriveDSN(adminDSN, name+"_own")
	if err != nil {
		t.Fatal(err)
	}

	admin, err := pg.Open(adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, stmt := range []string{
			"DROP DATABASE IF EXISTS " + pgQuoteIdent(name) + " WITH (FORCE)",
			"DROP DATABASE IF EXISTS " + pgQuoteIdent(name+"_own") + " WITH (FORCE)",
			"DROP ROLE IF EXISTS " + pgQuoteIdent(name),
		} {
			if _, err := admin.ExecContext(cleanup, stmt); err != nil {
				t.Errorf("cleanup %s: %v", stmt, err)
			}
		}
		_ = admin.Close()
	})

	t.Run("host preflight of a login that may create databases", func(t *testing.T) {
		report := pg.HostPreflight(ctx, adminDSN, "")
		if report.Dialect != "postgres" || report.User != dsnUser(t, adminDSN) {
			t.Errorf("report = %+v", report)
		}
		if report.CreateDB == nil || !*report.CreateDB || len(report.Details) != 0 {
			t.Errorf("create_db = %s, details %q", show(report.CreateDB), report.Details)
		}
		if report.DDL != nil || report.DML != nil {
			t.Errorf("a host report determines no DDL/DML: %+v", report)
		}
	})

	t.Run("ensure creates the login and its database through the admin DSN", func(t *testing.T) {
		res, err := pg.EnsureDatabase(ctx, targetDSN, adminDSN)
		if err != nil {
			t.Fatal(err)
		}
		if !res.LoginCreated || !res.DatabaseCreated || res.Name != name {
			t.Errorf("first ensure = %+v", res)
		}
		res, err = pg.EnsureDatabase(ctx, targetDSN, adminDSN)
		if err != nil || res.LoginCreated || res.DatabaseCreated {
			t.Errorf("second ensure = %+v, %v — want nothing created", res, err)
		}
	})

	t.Run("ensure creates a database with the DSN's own rights", func(t *testing.T) {
		res, err := pg.EnsureDatabase(ctx, ownDSN, "")
		if err != nil {
			t.Fatal(err)
		}
		if res.LoginCreated || !res.DatabaseCreated || res.Name != name+"_own" {
			t.Errorf("ensure = %+v", res)
		}
	})

	t.Run("ensure without the create right reports the database's refusal", func(t *testing.T) {
		missing, err := pg.DeriveDSN(baseDSN, name+"_refused")
		if err != nil {
			t.Fatal(err)
		}
		_, err = pg.EnsureDatabase(ctx, missing, "")
		if err == nil || !strings.Contains(err.Error(), "SQLSTATE 42501") {
			t.Errorf("err = %v, want permission denied (42501)", err)
		}
	})

	t.Run("host preflight of a login that may not create databases", func(t *testing.T) {
		report := pg.HostPreflight(ctx, baseDSN, "")
		if report.User != name || report.CreateDB == nil || *report.CreateDB {
			t.Fatalf("report = %+v", report)
		}
		want := `grant the create-database right with: ALTER ROLE "` + name + `" CREATEDB`
		if len(report.Details) != 1 || report.Details[0] != want {
			t.Errorf("details = %q, want the grant statement", report.Details)
		}
		// With an admin DSN the admin's right is the one that counts.
		report = pg.HostPreflight(ctx, baseDSN, adminDSN)
		if report.CreateDB == nil || !*report.CreateDB ||
			len(report.Details) != 1 || report.Details[0] != "database creation uses --admin-dsn" {
			t.Errorf("with --admin-dsn: %+v", report)
		}
	})

	db, err := pg.Open(targetDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("cannot reach the database ensure created: %v", err)
	}
	count := func(t *testing.T, table string) int {
		t.Helper()
		var n int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+pgQuoteIdent(table)).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	t.Run("preflight probes the rights and cleans up", func(t *testing.T) {
		report := pg.Preflight(ctx, targetDSN, db)
		if report.Dialect != "postgres" || report.User != name || report.Database != name {
			t.Errorf("identity = %+v", report)
		}
		if report.CreateDB == nil || *report.CreateDB {
			t.Errorf("create_db = %s, want false for a plain login", show(report.CreateDB))
		}
		if !sameBool(report.DDL, ptr(true)) || !sameBool(report.DML, ptr(true)) {
			t.Errorf("ddl = %s, dml = %s — details %q", show(report.DDL), show(report.DML), report.Details)
		}
		if report.Trigram == nil {
			t.Errorf("trigram not determined — details %q", report.Details)
		}
		var gone bool
		if err := db.QueryRowContext(ctx,
			"SELECT to_regclass('"+probeTable+"') IS NULL").Scan(&gone); err != nil || !gone {
			t.Errorf("probe table left behind (gone=%v, err=%v)", gone, err)
		}
	})

	t.Run("snapshot applies a whole script", func(t *testing.T) {
		script := `BEGIN;
CREATE TABLE IF NOT EXISTS "album" ("k" TEXT PRIMARY KEY, "_sync_revision" BIGINT);
INSERT INTO "album" ("k", "_sync_revision") VALUES ('Meddle', 1)
ON CONFLICT ("k") DO UPDATE SET "_sync_revision" = EXCLUDED."_sync_revision";
COMMIT;`
		for range 2 { // re-applying converges
			if err := pg.Snapshot(ctx, db, script); err != nil {
				t.Fatalf("snapshot: %s", pg.DescribeError(script, err))
			}
		}
		if n := count(t, "album"); n != 1 {
			t.Errorf("album holds %d row(s), want 1", n)
		}
	})

	t.Run("a failed snapshot applies nothing and leaves the pool usable", func(t *testing.T) {
		script := `BEGIN;
INSERT INTO "album" ("k", "_sync_revision") VALUES ('Animals', 1);
CREATE TABLE "track" ("k" nosuchtype);
COMMIT;`
		err := pg.Snapshot(ctx, db, script)
		if err == nil {
			t.Fatal("the script must fail")
		}
		described := pg.DescribeError(script, err)
		for _, want := range []string{"SQLSTATE 42704", `script line 3: CREATE TABLE "track" ("k" nosuchtype);`} {
			if !strings.Contains(described, want) {
				t.Errorf("missing %q in:\n%s", want, described)
			}
		}
		// The session that failed sits in an aborted transaction; it must not
		// be the one the next statement gets.
		if n := count(t, "album"); n != 1 {
			t.Errorf("album holds %d row(s) after a failed snapshot, want 1", n)
		}
	})

	t.Run("a failed window is rolled back whole", func(t *testing.T) {
		deltas := []framing.Delta{
			{ID: 1, Kind: "data", Op: "upsert",
				SQL: `INSERT INTO "album" ("k", "_sync_revision") VALUES ('Animals', 2);`},
			{ID: 2, Kind: "data", Op: "upsert",
				SQL: `INSERT INTO "album" ("k", "_sync_revision") VALUES ('Wish', 2);
INSERT INTO "nope" ("k") VALUES ('b');`},
		}
		res, err := Batch(ctx, db, deltas, false)
		if err == nil || res.FailedID == nil || *res.FailedID != 2 {
			t.Fatalf("failed delta = %v, err = %v — want delta 2", res.FailedID, err)
		}
		described := pg.DescribeError(deltas[1].SQL, err)
		for _, want := range []string{
			"delta 2 (data upsert): ", "SQLSTATE 42P01", `script line 2: INSERT INTO "nope" ("k") VALUES ('b');`,
		} {
			if !strings.Contains(described, want) {
				t.Errorf("missing %q in:\n%s", want, described)
			}
		}
		if n := count(t, "album"); n != 1 {
			t.Errorf("rollback left %d row(s), want 1", n)
		}

		res, err = Batch(ctx, db, deltas[:1], false)
		if err != nil || res.LastID != 1 {
			t.Fatalf("applying the sound delta alone: %+v, %v", res, err)
		}
		if n := count(t, "album"); n != 2 {
			t.Errorf("album holds %d row(s) after the sound delta, want 2", n)
		}
	})
}

// dsnUser reads the login a DSN names.
func dsnUser(t *testing.T, dsn string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	return u.User.Username()
}

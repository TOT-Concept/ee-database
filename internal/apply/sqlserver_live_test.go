package apply

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/TOT-Concept/ee-database/internal/framing"
)

// The live tests run the driver against a real SQL Server 2022. They need a
// sysadmin DSN that names no database in EE_TEST_SQLSERVER_DSN (the variable the
// backend's conformance suite reads too) and skip without it: the public CI
// has no database. Databases and logins they create are named
// ee_t222_go_<random> and dropped at the end.

// snapshotScript is a snapshot in the server's layout: one transaction over
// GO-separated batches, each refusing to run unless the one before it did.
// The data batch is the caller's.
func snapshotScript(data string) string {
	return `-- Entity Enricher snapshot (dialect sqlserver)
SET XACT_ABORT ON; SET QUOTED_IDENTIFIER ON; SET ANSI_NULLS ON;
EXEC sp_set_session_context N'ee_snapshot', 0;
BEGIN TRANSACTION;
GO
IF @@TRANCOUNT = 0 OR CAST(SESSION_CONTEXT(N'ee_snapshot') AS INT) <> 0 THROW 50002, N'EE-SNAPSHOT: an earlier batch failed — nothing was applied', 1;
EXEC sp_set_session_context N'ee_snapshot', 1;
IF OBJECT_ID(N'album', N'U') IS NULL CREATE TABLE [album] ([k] NVARCHAR(400) COLLATE Latin1_General_100_BIN2 NOT NULL, [_sync_revision] BIGINT NOT NULL DEFAULT 0, UNIQUE ([k]));
GO
IF @@TRANCOUNT = 0 OR CAST(SESSION_CONTEXT(N'ee_snapshot') AS INT) <> 1 THROW 50002, N'EE-SNAPSHOT: an earlier batch failed — nothing was applied', 1;
EXEC sp_set_session_context N'ee_snapshot', 2;
IF COL_LENGTH(N'album', N'year') IS NULL ALTER TABLE [album] ADD [year] INT NULL;
GO
-- a batch may hold nothing but a comment
GO

IF @@TRANCOUNT = 0 OR CAST(SESSION_CONTEXT(N'ee_snapshot') AS INT) <> 2 THROW 50002, N'EE-SNAPSHOT: an earlier batch failed — nothing was applied', 1;
EXEC sp_set_session_context N'ee_snapshot', 3;
` + data + `
GO
IF @@TRANCOUNT = 0 OR CAST(SESSION_CONTEXT(N'ee_snapshot') AS INT) <> 3 THROW 50002, N'EE-SNAPSHOT: an earlier batch failed — nothing was applied', 1;
COMMIT;
GO
`
}

// albumMerge is the upsert form the server renders, for one album row.
func albumMerge(key string, year, revision int) string {
	lit := msQuoteLiteral(key)
	return "MERGE INTO [album] AS t USING (SELECT " + lit + " AS [k], " + strconv.Itoa(year) + " AS [year], " +
		strconv.Itoa(revision) + " AS [_sync_revision]) AS s ON (t.[k] IS NOT DISTINCT FROM s.[k]) " +
		"WHEN MATCHED AND t.[_sync_revision] < s.[_sync_revision] AND (t.[year] IS DISTINCT FROM s.[year]) " +
		"THEN UPDATE SET [year] = s.[year], [_sync_revision] = s.[_sync_revision] " +
		"WHEN NOT MATCHED THEN INSERT ([k], [year], [_sync_revision]) VALUES (s.[k], s.[year], s.[_sync_revision]);"
}

func TestSQLServerLive(t *testing.T) {
	adminDSN := os.Getenv("EE_TEST_SQLSERVER_DSN")
	if adminDSN == "" {
		t.Skip("EE_TEST_SQLSERVER_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	ms, err := For("sqlserver")
	if err != nil {
		t.Fatal(err)
	}

	// A login that does not exist yet, on the admin DSN's server: the base
	// DSN of a managed host, and the target DSN `--admin-dsn` provisions.
	name := liveName(t)
	const password = `Ee_r'eplica"123` // passes the password policy; quotes to escape
	withLogin := func(login, pass string) string {
		u, err := url.Parse(adminDSN)
		if err != nil {
			t.Fatal(err)
		}
		u.User = url.UserPassword(login, pass)
		return u.String()
	}
	derive := func(base, database string) string {
		dsn, err := ms.DeriveDSN(base, database)
		if err != nil {
			t.Fatal(err)
		}
		return dsn
	}
	baseDSN := withLogin(name, password)
	targetDSN := derive(baseDSN, name)
	ownDSN := derive(adminDSN, name+"_own")

	admin, err := ms.Open(derive(adminDSN, "master"))
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("cannot reach the server: %v", err)
	}
	exists := func(t *testing.T, query, arg string) bool {
		t.Helper()
		var found bool
		if err := admin.QueryRowContext(ctx, query, arg).Scan(&found); err != nil {
			t.Fatal(err)
		}
		return found
	}
	databaseExists := func(t *testing.T, database string) bool {
		t.Helper()
		return exists(t, "SELECT CAST(CASE WHEN DB_ID(@p1) IS NULL THEN 0 ELSE 1 END AS BIT)", database)
	}
	loginExists := func(t *testing.T, login string) bool {
		t.Helper()
		return exists(t, "SELECT CAST(CASE WHEN SUSER_ID(@p1) IS NULL THEN 0 ELSE 1 END AS BIT)", login)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		for _, suffix := range []string{"", "_own", "_b", "_weak", "_refused"} {
			database := msQuoteIdent(name + suffix)
			stmt := "IF DB_ID(" + msQuoteLiteral(name+suffix) + ") IS NOT NULL BEGIN ALTER DATABASE " + database +
				" SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE " + database + "; END"
			if _, err := admin.ExecContext(cleanup, stmt); err != nil {
				t.Errorf("cleanup %s: %v", stmt, err)
			}
		}
		for _, login := range []string{name, name + "_weak"} {
			stmt := "IF SUSER_ID(" + msQuoteLiteral(login) + ") IS NOT NULL DROP LOGIN " + msQuoteIdent(login)
			// A login is refused while one of its sessions is still closing.
			var err error
			for range 20 {
				if _, err = admin.ExecContext(cleanup, stmt); err == nil {
					break
				}
				time.Sleep(250 * time.Millisecond)
			}
			if err != nil {
				t.Errorf("cleanup %s: %v", stmt, err)
			}
		}
		_ = admin.Close()
	})

	t.Run("host preflight of a login that may create databases", func(t *testing.T) {
		report := ms.HostPreflight(ctx, adminDSN, "")
		if report.Dialect != "sqlserver" || report.User != dsnUser(t, adminDSN) {
			t.Errorf("report = %+v", report)
		}
		if report.CreateDB == nil || !*report.CreateDB || len(report.Details) != 0 {
			t.Errorf("create_db = %s, details %q", show(report.CreateDB), report.Details)
		}
		if report.DDL != nil || report.DML != nil || report.Trigram != nil {
			t.Errorf("a host report determines no DDL/DML: %+v", report)
		}
	})

	// The server's password policy is on by default (CHECK_POLICY = ON): the
	// refusal is the server's own sentence, and nothing is left half-made.
	// The error number depends on who enforces the policy (15118 where Windows
	// does, 33062 / 33064 on Linux), so the reference is what this server
	// answers to the same statement.
	t.Run("a login whose password the server's policy refuses", func(t *testing.T) {
		weak := name + "_weak"
		for _, pass := range []string{"abc", "abcdefghij"} {
			_, direct := admin.ExecContext(ctx,
				"CREATE LOGIN "+msQuoteIdent(weak)+" WITH PASSWORD = "+msQuoteLiteral(pass))
			if direct == nil || !strings.HasPrefix(direct.Error(), "mssql: Password validation failed. ") {
				t.Fatalf("the server accepted the password %q (err = %v): no policy to test", pass, direct)
			}
			_, err := ms.EnsureDatabase(ctx, derive(withLogin(weak, pass), weak), adminDSN)
			t.Logf("password %q: %v", pass, err)
			if want := `create login "` + weak + `": ` + direct.Error(); err == nil || err.Error() != want {
				t.Errorf("err = %v\nwant %s", err, want)
			}
			if loginExists(t, weak) || databaseExists(t, weak) {
				t.Errorf("a refused login left something behind")
			}
		}
	})

	t.Run("ensure creates the login and its database through the admin DSN", func(t *testing.T) {
		res, err := ms.EnsureDatabase(ctx, targetDSN, adminDSN)
		if err != nil {
			t.Fatal(err)
		}
		if !res.LoginCreated || !res.DatabaseCreated || res.Name != name {
			t.Errorf("first ensure = %+v", res)
		}
		res, err = ms.EnsureDatabase(ctx, targetDSN, adminDSN)
		if err != nil || res.LoginCreated || res.DatabaseCreated {
			t.Errorf("second ensure = %+v, %v — want nothing created", res, err)
		}
	})

	// A managed host creates one database per registration with one login:
	// the second database must be open to the login that already exists.
	second := derive(baseDSN, name+"_b")
	isOwner := func(t *testing.T) (bool, error) {
		t.Helper()
		db, err := ms.Open(second)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		var owner bool
		err = db.QueryRowContext(ctx, "SELECT CAST(IS_ROLEMEMBER('db_owner') AS BIT)").Scan(&owner)
		return owner, err
	}
	inSecond := func(t *testing.T, stmt string) {
		t.Helper()
		// Back to master in the same batch: the pooled session must not stay
		// in a database the cleanup drops.
		if _, err := admin.ExecContext(ctx,
			"USE "+msQuoteIdent(name+"_b")+"; "+stmt+"; USE master;"); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	t.Run("ensure opens a second database to the existing login", func(t *testing.T) {
		res, err := ms.EnsureDatabase(ctx, second, adminDSN)
		if err != nil {
			t.Fatal(err)
		}
		if res.LoginCreated || !res.DatabaseCreated || res.Name != name+"_b" {
			t.Errorf("ensure = %+v", res)
		}
		if owner, err := isOwner(t); err != nil || !owner {
			t.Errorf("the login is not db_owner of the second database (owner=%v, err=%v)", owner, err)
		}
	})

	// A call that stopped between CREATE DATABASE and CREATE USER (Ctrl-C, a
	// dropped admin connection) leaves a login and a database that both exist,
	// and a database the login cannot open: the next call finishes the job.
	t.Run("ensure lets the login into a database an interrupted call left closed", func(t *testing.T) {
		inSecond(t, "DROP USER "+msQuoteIdent(name))
		if _, err := isOwner(t); err == nil || !strings.Contains(err.Error(), "(4063)") {
			t.Fatalf("err = %v, want the login refused by the database (4063)", err)
		}
		res, err := ms.EnsureDatabase(ctx, second, adminDSN)
		if err != nil || res.LoginCreated || res.DatabaseCreated {
			t.Fatalf("ensure = %+v, %v — want nothing created", res, err)
		}
		if owner, err := isOwner(t); err != nil || !owner {
			t.Errorf("the login is not db_owner after the retry (owner=%v, err=%v)", owner, err)
		}
	})

	// A login that already has a user in the database keeps the rights its
	// owner gave it: nothing is added behind the owner's back.
	t.Run("ensure leaves the rights of an existing user alone", func(t *testing.T) {
		inSecond(t, "ALTER ROLE db_owner DROP MEMBER "+msQuoteIdent(name))
		if _, err := ms.EnsureDatabase(ctx, second, adminDSN); err != nil {
			t.Fatal(err)
		}
		if owner, err := isOwner(t); err != nil || owner {
			t.Errorf("owner=%v, err=%v — want the user left without db_owner", owner, err)
		}
	})

	t.Run("ensure creates a database with the DSN's own rights", func(t *testing.T) {
		res, err := ms.EnsureDatabase(ctx, ownDSN, "")
		if err != nil {
			t.Fatal(err)
		}
		if res.LoginCreated || !res.DatabaseCreated || res.Name != name+"_own" {
			t.Errorf("ensure = %+v", res)
		}
	})

	t.Run("ensure without the create right reports the database's refusal", func(t *testing.T) {
		refused := name + "_refused"
		_, err := ms.EnsureDatabase(ctx, derive(baseDSN, refused), "")
		want := `create database "` + refused + `": mssql: CREATE DATABASE permission denied in database 'master'. (262)`
		if err == nil || err.Error() != want {
			t.Errorf("err = %v\nwant %s", err, want)
		}
	})

	t.Run("host preflight of a login that may not create databases", func(t *testing.T) {
		report := ms.HostPreflight(ctx, baseDSN, "")
		if report.User != name || report.CreateDB == nil || *report.CreateDB {
			t.Fatalf("report = %+v", report)
		}
		const hint = "grant the create-database right with: "
		grant := "USE master; GRANT CREATE ANY DATABASE TO " + msQuoteIdent(name)
		if len(report.Details) != 1 || report.Details[0] != hint+grant {
			t.Fatalf("details = %q, want the grant statement", report.Details)
		}
		// With an admin DSN the admin's right is the one that counts.
		report = ms.HostPreflight(ctx, baseDSN, adminDSN)
		if report.CreateDB == nil || !*report.CreateDB ||
			len(report.Details) != 1 || report.Details[0] != "database creation uses --admin-dsn" {
			t.Errorf("with --admin-dsn: %+v", report)
		}
		// The statement the report gives is the one that grants the right.
		if _, err := admin.ExecContext(ctx, grant); err != nil {
			t.Fatalf("the grant statement of the report failed: %v", err)
		}
		report = ms.HostPreflight(ctx, baseDSN, "")
		if report.CreateDB == nil || !*report.CreateDB || len(report.Details) != 0 {
			t.Errorf("after the grant: %+v", report)
		}
		if _, err := admin.ExecContext(ctx,
			"USE master; REVOKE CREATE ANY DATABASE FROM "+msQuoteIdent(name)); err != nil {
			t.Fatal(err)
		}
	})

	db, err := ms.Open(targetDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("cannot reach the database ensure created: %v", err)
	}
	scalar := func(t *testing.T, pool *sql.DB, query string) int {
		t.Helper()
		var n int
		if err := pool.QueryRowContext(ctx, query).Scan(&n); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return n
	}
	albums := func(t *testing.T) int {
		t.Helper()
		return scalar(t, db, "SELECT COUNT(*) FROM [album]")
	}
	const noProbeTable = "SELECT CASE WHEN OBJECT_ID(N'" + probeTable + "', N'U') IS NULL THEN 1 ELSE 0 END"

	t.Run("preflight probes the rights and cleans up", func(t *testing.T) {
		report := ms.Preflight(ctx, targetDSN, db)
		if report.Dialect != "sqlserver" || report.User != name || report.Database != name {
			t.Errorf("identity = %+v", report)
		}
		if report.CreateDB == nil || *report.CreateDB {
			t.Errorf("create_db = %s, want false for a plain login", show(report.CreateDB))
		}
		if !sameBool(report.DDL, ptr(true)) || !sameBool(report.DML, ptr(true)) || len(report.Details) != 0 {
			t.Errorf("ddl = %s, dml = %s — details %q", show(report.DDL), show(report.DML), report.Details)
		}
		if report.Trigram != nil {
			t.Errorf("SQL Server has no trigram extension to report: %s", show(report.Trigram))
		}
		if scalar(t, db, noProbeTable) != 1 {
			t.Error("probe table left behind")
		}
	})

	// The rendered SQL needs OPENJSON, which a database left at compatibility
	// level 120 does not have: the rights are there, the scripts would fail.
	t.Run("preflight refuses a database at an old compatibility level", func(t *testing.T) {
		if _, err := admin.ExecContext(ctx,
			"ALTER DATABASE "+msQuoteIdent(name+"_own")+" SET COMPATIBILITY_LEVEL = 120"); err != nil {
			t.Fatal(err)
		}
		old, err := ms.Open(ownDSN)
		if err != nil {
			t.Fatal(err)
		}
		defer old.Close()
		report := ms.Preflight(ctx, ownDSN, old)
		if !sameBool(report.CreateDB, ptr(true)) || !sameBool(report.DDL, ptr(false)) || !sameBool(report.DML, ptr(true)) {
			t.Errorf("create_db = %s, ddl = %s, dml = %s", show(report.CreateDB), show(report.DDL), show(report.DML))
		}
		want := "SQL Server 2022 and compatibility level 130 or later are required " +
			"(this database: major version 16, compatibility level 120)"
		if len(report.Details) != 1 || report.Details[0] != want {
			t.Errorf("details = %q", report.Details)
		}
	})

	t.Run("snapshot applies its batches in one transaction", func(t *testing.T) {
		script := snapshotScript(albumMerge("Meddle", 1971, 1) + "\n" + albumMerge("日本語 'GO'", 1977, 1))
		for range 2 { // re-applying converges
			if err := ms.Snapshot(ctx, db, script); err != nil {
				t.Fatalf("snapshot: %s", ms.DescribeError(script, err))
			}
		}
		if n := albums(t); n != 2 {
			t.Errorf("album holds %d row(s), want 2", n)
		}
		if n := scalar(t, db, "SELECT COUNT(*) FROM [album] WHERE [k] = N'日本語 ''GO''' AND [year] = 1977"); n != 1 {
			t.Errorf("the Unicode key did not round-trip")
		}
		// The connection went back to the pool with no transaction left open.
		if n := scalar(t, db, "SELECT @@TRANCOUNT"); n != 0 {
			t.Errorf("@@TRANCOUNT = %d after a snapshot", n)
		}
	})

	// failedSnapshot applies a script that must fail and checks that nothing
	// of it stayed: the [year2] column its second batch would add, and rows.
	failedSnapshot := func(t *testing.T, script string) (*BatchError, string) {
		t.Helper()
		before := albums(t)
		err := ms.Snapshot(ctx, db, script)
		var batchErr *BatchError
		if !errors.As(err, &batchErr) {
			t.Fatalf("err = %v, want a *BatchError", err)
		}
		if n := albums(t); n != before {
			t.Errorf("album holds %d row(s) after a failed snapshot, want %d", n, before)
		}
		if n := scalar(t, db, "SELECT CASE WHEN COL_LENGTH(N'album', N'year2') IS NULL THEN 0 ELSE 1 END"); n != 0 {
			t.Error("the column an earlier batch added survived the failure")
		}
		// The session that failed held the transaction; it must not be the
		// one the next statement gets.
		if n := scalar(t, db, "SELECT @@TRANCOUNT"); n != 0 {
			t.Errorf("@@TRANCOUNT = %d on the next pooled connection", n)
		}
		return batchErr, ms.DescribeError(script, err)
	}
	// Same layout as snapshotScript, with a column added before the data.
	failing := func(data string) string {
		return strings.Replace(snapshotScript(data),
			"IF COL_LENGTH(N'album', N'year') IS NULL ALTER TABLE [album] ADD [year] INT NULL;",
			"IF COL_LENGTH(N'album', N'year2') IS NULL ALTER TABLE [album] ADD [year2] INT NULL;", 1)
	}
	lineOf := func(t *testing.T, script, statement string) int {
		t.Helper()
		head, _, found := strings.Cut(script, statement)
		if !found {
			t.Fatalf("statement not in the script: %s", statement)
		}
		return strings.Count(head, "\n") + 1
	}

	t.Run("a failing data batch names its line of the whole script", func(t *testing.T) {
		// The second row breaks the unique key (the MERGE never matches).
		duplicate := "MERGE INTO [album] AS t USING (SELECT N'Meddle' AS [k]) AS s ON (1 = 0) " +
			"WHEN NOT MATCHED THEN INSERT ([k]) VALUES (s.[k]);"
		script := failing(albumMerge("Animals", 1977, 2) + "\n" + duplicate + "\n" + albumMerge("Wish", 1975, 2))
		batchErr, described := failedSnapshot(t, script)
		// Batches: begin, creates, convergence, the comment, data.
		if batchErr.Index != 4 || batchErr.StartLine != 16 {
			t.Errorf("batch index %d, start line %d", batchErr.Index, batchErr.StartLine)
		}
		t.Logf("described:\n%s", described)
		for _, want := range []string{
			"batch 5: mssql: Violation of UNIQUE KEY constraint ",
			"The duplicate key value is (Meddle). (2627)",
			"\n  number: 2627", "\n  state: 1",
			"\n  script line " + strconv.Itoa(lineOf(t, script, duplicate)) + ": " + duplicate,
		} {
			if !strings.Contains(described, want) {
				t.Errorf("missing %q in:\n%s", want, described)
			}
		}
	})

	t.Run("a migration guard raised inside EXEC arrives word for word", func(t *testing.T) {
		guard := "EXEC(N'DECLARE @n BIGINT = (SELECT COUNT(*) FROM [album]); IF @n > 0 BEGIN DECLARE @m NVARCHAR(2048) = " +
			"CONCAT(N''EE-MIGRATE: '', @n, N'' row(s) of album.k block the migration — it''''s refused''); THROW 50001, @m, 1; END;');"
		script := failing(guard)
		_, described := failedSnapshot(t, script)
		t.Logf("described:\n%s", described)
		// The EXEC reports its own line 1. The batch opens with a blank line,
		// which cannot have raised it: the EXEC is the line.
		want := "batch 5: mssql: EE-MIGRATE: 2 row(s) of album.k block the migration — it's refused (50001)\n" +
			"  number: 50001\n  state: 1\n" +
			"  script line " + strconv.Itoa(lineOf(t, script, guard)) + ": " + guard
		if described != want {
			t.Errorf("got:\n%s\nwant:\n%s", described, want)
		}
	})

	t.Run("a batch the server cannot parse stops the snapshot", func(t *testing.T) {
		// CREATE TABLE IF NOT EXISTS is not T-SQL. A parse error skips the
		// batch without ending the transaction; the client stops there.
		parse := "CREATE TABLE IF NOT EXISTS [track] ([k] INT);"
		script := failing(albumMerge("Animals", 1977, 2) + "\n" + parse)
		_, described := failedSnapshot(t, script)
		t.Logf("described:\n%s", described)
		for _, want := range []string{
			"(156)", "\n  number: 156",
			"\n  script line " + strconv.Itoa(lineOf(t, script, parse)) + ": " + parse,
		} {
			if !strings.Contains(described, want) {
				t.Errorf("missing %q in:\n%s", want, described)
			}
		}
	})

	const prefix = "SET XACT_ABORT ON; SET QUOTED_IDENTIFIER ON;\n"

	t.Run("a failed window is rolled back whole", func(t *testing.T) {
		duplicate := "MERGE INTO [album] AS t USING (SELECT N'Meddle' AS [k]) AS s ON (1 = 0) " +
			"WHEN NOT MATCHED THEN INSERT ([k]) VALUES (s.[k]);"
		deltas := []framing.Delta{
			{ID: 1, Kind: "data", Op: "upsert", SQL: prefix + albumMerge("Animals", 1977, 2)},
			{ID: 2, Kind: "data", Op: "upsert", SQL: prefix + albumMerge("Wish", 1975, 2) + "\n" + duplicate},
		}
		res, err := Batch(ctx, db, deltas, false)
		if err == nil || res.FailedID == nil || *res.FailedID != 2 {
			t.Fatalf("failed delta = %v, err = %v — want delta 2", res.FailedID, err)
		}
		described := ms.DescribeError(deltas[1].SQL, err)
		t.Logf("described:\n%s", described)
		for _, want := range []string{
			"delta 2 (data upsert): mssql: Violation of UNIQUE KEY constraint ",
			"\n  number: 2627", "\n  state: 1", "\n  script line 3: " + duplicate,
		} {
			if !strings.Contains(described, want) {
				t.Errorf("missing %q in:\n%s", want, described)
			}
		}
		if n := albums(t); n != 2 {
			t.Errorf("rollback left %d row(s), want 2", n)
		}

		res, err = Batch(ctx, db, deltas[:1], false)
		if err != nil || res.LastID != 1 {
			t.Fatalf("applying the sound delta alone: %+v, %v", res, err)
		}
		if n := albums(t); n != 3 {
			t.Errorf("album holds %d row(s) after the sound delta, want 3", n)
		}
		if got := res.Shape.Summary(0); got != "1 statement(s): album 1 upsert" {
			t.Errorf("shape = %q", got)
		}
	})

	// A migration delta: DDL is transactional, and the guard's THROW undoes
	// the column the statement before it added.
	t.Run("a THROW in a migration delta rolls its DDL back and names its statement", func(t *testing.T) {
		guard := "EXEC(N'DECLARE @n BIGINT = (SELECT COUNT(*) FROM [album] WHERE [label] IS NULL); IF @n > 0 BEGIN " +
			"DECLARE @m NVARCHAR(2048) = CONCAT(N''EE-MIGRATE: NULL values in album block NOT NULL on label ('', @n, N'' row(s))''); " +
			"THROW 50001, @m, 1; END;');"
		deltas := []framing.Delta{{ID: 7, Kind: "schema", Op: "migrate",
			SQL: prefix + "-- album.label becomes required\n" +
				"EXEC(N'ALTER TABLE [album] ADD [label] NVARCHAR(400) NULL;');\n" + guard}}
		res, err := Batch(ctx, db, deltas, false)
		if err == nil || res.FailedID == nil || *res.FailedID != 7 {
			t.Fatalf("failed delta = %v, err = %v — want delta 7", res.FailedID, err)
		}
		described := ms.DescribeError(deltas[0].SQL, err)
		t.Logf("described:\n%s", described)
		// Two EXEC lines: either may have raised "line 1", so none is named.
		want := "delta 7 (schema migrate): mssql: EE-MIGRATE: NULL values in album block NOT NULL on label (3 row(s)) (50001)\n" +
			"  number: 50001\n  state: 1"
		if described != want {
			t.Errorf("got:\n%s\nwant:\n%s", described, want)
		}
		if n := scalar(t, db, "SELECT CASE WHEN COL_LENGTH(N'album', N'label') IS NULL THEN 0 ELSE 1 END"); n != 0 {
			t.Error("the column added before the THROW survived the rollback")
		}

		// The guard alone is the only statement that can have raised it.
		alone := []framing.Delta{{ID: 8, Kind: "schema", Op: "migrate",
			SQL: prefix + strings.Replace(guard, "WHERE [label] IS NULL", "", 1)}}
		_, err = Batch(ctx, db, alone, false)
		described = ms.DescribeError(alone[0].SQL, err)
		if !strings.HasSuffix(described, "\n  script line 2: "+strings.Replace(guard, "WHERE [label] IS NULL", "", 1)) {
			t.Errorf("got:\n%s", described)
		}
		// The pool is sound after two aborted transactions.
		if n := albums(t); n != 3 {
			t.Errorf("album holds %d row(s), want 3", n)
		}
	})
}

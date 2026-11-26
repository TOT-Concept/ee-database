package apply

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	mssql "github.com/microsoft/go-mssqldb"
)

func TestForResolvesSQLServer(t *testing.T) {
	driver, err := For("sqlserver")
	if err != nil {
		t.Fatal(err)
	}
	if driver.Dialect() != "sqlserver" || driver.Label() != "SQL Server" {
		t.Errorf("For(sqlserver) = %s / %s", driver.Dialect(), driver.Label())
	}
}

func TestSQLServerDeriveDSN(t *testing.T) {
	ms := sqlServerDriver{}
	cases := []struct{ name, base, database, want string }{
		{"a server", "sqlserver://ee_replica:pw@localhost:15433", "vinyl_db",
			"sqlserver://ee_replica:pw@localhost:15433?database=vinyl_db"},
		// The other parameters survive; a database the base DSN named is
		// replaced, however it was spelled.
		{"with parameters", "sqlserver://u:p@h:1433?TrustServerCertificate=true&Database=master", "my_db",
			"sqlserver://u:p@h:1433?TrustServerCertificate=true&database=my_db"},
		{"a named instance", " sqlserver://u:p%40ss@h/SQLEXPRESS?encrypt=disable ", "films__1",
			"sqlserver://u:p%40ss@h/SQLEXPRESS?database=films__1&encrypt=disable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ms.DeriveDSN(tc.base, tc.database)
			if err != nil || got != tc.want {
				t.Errorf("got %q err=%v, want %q", got, err, tc.want)
			}
			// What is derived is a DSN the driver reads back.
			cfg, err := sqlServerTarget(got)
			if err != nil || cfg.Database != tc.database {
				t.Errorf("read back: database %q, err=%v", cfg.Database, err)
			}
		})
	}
}

func TestSQLServerDeriveDSNRejects(t *testing.T) {
	ms := sqlServerDriver{}
	if _, err := ms.DeriveDSN("sqlserver://u:p@h", ""); err == nil {
		t.Error("want error for empty database name")
	}
	for _, base := range []string{
		"server=h;user id=u;password=secret", "postgres://u:secret@h/", "sqlserver://u:secret@h:port",
	} {
		_, err := ms.DeriveDSN(base, "db")
		if err == nil {
			t.Errorf("want error for base dsn %q", base)
		} else if strings.Contains(err.Error(), "secret") {
			t.Errorf("the error quotes the password: %v", err)
		}
	}
}

func TestSQLServerOpenRejectsOtherDSNForms(t *testing.T) {
	for _, dsn := range []string{
		"server=localhost;user id=sa;password=secret",
		"odbc:server=localhost;user id=sa;password=secret",
		"postgres://u:secret@h/db",
		"sqlserver://sa:secret@localhost:notaport",
	} {
		db, err := sqlServerDriver{}.Open(dsn)
		if err == nil {
			_ = db.Close()
			t.Errorf("Open(%q) must fail", dsn)
			continue
		}
		if !strings.HasPrefix(err.Error(), "sqlserver dsn: ") || strings.Contains(err.Error(), "secret") {
			t.Errorf("Open(%q) error = %v", dsn, err)
		}
	}
}

// Without a database the server opens the login's default one — master, for
// most logins — and the replica's tables would be created there.
func TestSQLServerOpenRefusesADSNWithoutDatabase(t *testing.T) {
	const want = "sqlserver dsn names no database: add ?database=name " +
		"(a path after the host is an instance name)"
	for _, dsn := range []string{
		"sqlserver://sa:secret@localhost:1433",
		"sqlserver://sa:secret@localhost:1433?TrustServerCertificate=true",
		// The PostgreSQL way of writing it: the path is an instance name.
		"sqlserver://sa:secret@localhost:1433/my_replica",
		"sqlserver://sa:secret@localhost:1433?database=",
	} {
		db, err := sqlServerDriver{}.Open(dsn)
		if err == nil {
			_ = db.Close()
			t.Errorf("Open(%q) must fail", dsn)
		} else if err.Error() != want {
			t.Errorf("Open(%q) error = %v", dsn, err)
		}
	}
	db, err := sqlServerDriver{}.Open("sqlserver://sa:secret@localhost:1433/SQLEXPRESS?Database=my_replica")
	if err != nil {
		t.Fatalf("a named instance with a database must open: %v", err)
	}
	_ = db.Close()
}

func TestSQLServerQuoting(t *testing.T) {
	if got := msQuoteIdent("video_games"); got != "[video_games]" {
		t.Errorf("msQuoteIdent = %s", got)
	}
	if got := msQuoteIdent("we]ird"); got != "[we]]ird]" {
		t.Errorf("msQuoteIdent escaping = %s", got)
	}
	if got := msQuoteLiteral("pa'ss"); got != "N'pa''ss'" {
		t.Errorf("msQuoteLiteral = %s", got)
	}
}

func TestSQLServerEnsureDatabaseBadDSNs(t *testing.T) {
	ms := sqlServerDriver{}
	ctx := context.Background()
	_, err := ms.EnsureDatabase(ctx, "server=h;database=x", "")
	if err == nil || !strings.HasPrefix(err.Error(), "sqlserver dsn: ") {
		t.Errorf("err = %v, want a dsn error", err)
	}
	// No database to create: said before any connection is attempted.
	_, err = ms.EnsureDatabase(ctx, "sqlserver://u:p@localhost:1", "")
	if err == nil || !strings.Contains(err.Error(), "names no database") {
		t.Errorf("err = %v, want the missing database name", err)
	}
	_, err = ms.EnsureDatabase(ctx, "sqlserver://u:p@localhost:1?database=x", "server=h")
	if err == nil || !strings.HasPrefix(err.Error(), "sqlserver admin dsn: ") {
		t.Errorf("err = %v, want an admin dsn error", err)
	}
}

func TestSQLServerHostPreflightBadDSN(t *testing.T) {
	report := sqlServerDriver{}.HostPreflight(context.Background(), "server=h;user id=u", "")
	if report.Dialect != "sqlserver" || report.CreateDB != nil {
		t.Errorf("an unreadable base dsn must determine nothing: %+v", report)
	}
	// A base DSN names no database: the form shown has none.
	if want := []string{"base dsn: it must be a URL (sqlserver://user:pass@host:port)"}; !slices.Equal(report.Details, want) {
		t.Errorf("details = %q, want %q", report.Details, want)
	}
	// With an admin DSN, that is the one provisioning reads.
	report = sqlServerDriver{}.HostPreflight(context.Background(), "sqlserver://u:p@h:1433", "server=h;user id=sa")
	if want := []string{
		"database creation uses --admin-dsn", "admin dsn: it must be a URL (sqlserver://user:pass@host:port)",
	}; report.CreateDB != nil || !slices.Equal(report.Details, want) {
		t.Errorf("details = %q, want %q", report.Details, want)
	}
}

func TestSQLServerVersionProblem(t *testing.T) {
	cases := []struct {
		major, edition, level int
		supported             bool
	}{
		{16, 3, 160, true}, {17, 3, 170, true}, {16, 2, 130, true},
		{16, 3, 120, false}, // SQL Server 2022, database left at an old level: no OPENJSON
		{15, 3, 150, false}, // SQL Server 2019: no IS DISTINCT FROM
		{12, 3, 120, false}, // SQL Server 2014: the major version Azure reports, on premises
		{12, 5, 170, true},  // Azure SQL Database
		{12, 8, 150, true},  // Azure SQL Managed Instance
		{12, 5, 120, false}, // Azure SQL Database left at an old level
		{12, 8, 120, false}, // Managed Instance left at an old level
	}
	for _, tc := range cases {
		problem := sqlServerVersionProblem(tc.major, tc.edition, tc.level)
		if (problem == "") != tc.supported {
			t.Errorf("version %d edition %d level %d: problem = %q",
				tc.major, tc.edition, tc.level, problem)
		}
		if problem != "" && !strings.HasPrefix(problem,
			"SQL Server 2022 and compatibility level 130 or later are required") {
			t.Errorf("problem = %q", problem)
		}
	}
}

func TestSplitBatches(t *testing.T) {
	type batch struct {
		line int
		sql  string
	}
	cases := []struct {
		name   string
		script string
		want   []batch
	}{
		{"no separator", "SELECT 1;\nSELECT 2;", []batch{{1, "SELECT 1;\nSELECT 2;"}}},
		{"the snapshot layout",
			"-- header\nBEGIN TRANSACTION;\nGO\nCREATE TABLE t (k int);\nGO\nCOMMIT;\nGO\n",
			[]batch{{1, "-- header\nBEGIN TRANSACTION;"}, {4, "CREATE TABLE t (k int);"}, {6, "COMMIT;"}}},
		{"any case, spaces around, CRLF",
			"SELECT 1;\r\n  go\t\r\nSELECT 2;\r\nGo\r\nSELECT 3;",
			[]batch{{1, "SELECT 1;"}, {3, "SELECT 2;"}, {5, "SELECT 3;"}}},
		// The lines a batch opens with are sent: the server counts from them.
		{"leading lines are kept", "GO\n\n-- note\nSELECT 1;\nGO", []batch{{2, "\n-- note\nSELECT 1;"}}},
		{"empty batches are dropped", "GO\nGO\n  \nGO\nSELECT 1;\n\n\nGO\n\n", []batch{{5, "SELECT 1;"}}},
		{"GO needs its own line", "SELECT 1; GO\nGOTO done;\nGO 5\n-- GO\nSELECT N'GO';",
			[]batch{{1, "SELECT 1; GO\nGOTO done;\nGO 5\n-- GO\nSELECT N'GO';"}}},
		{"nothing at all", " \n\n", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got []batch
			for _, b := range splitBatches(tc.script) {
				got = append(got, batch{b.startLine, b.sql})
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("got  %q\nwant %q", got, tc.want)
			}
		})
	}
}

func TestSQLServerSnapshotRunsItsBatchesOnOneConnection(t *testing.T) {
	db, fake := openFake(t)
	script := "BEGIN TRANSACTION;\nGO\nCREATE TABLE t (k int);\nGO\nCOMMIT;\nGO\n"
	if err := (sqlServerDriver{}).Snapshot(context.Background(), db, script); err != nil {
		t.Fatal(err)
	}
	// One Exec per batch, no GO sent, and the connection goes back to the pool.
	want := []string{"BEGIN TRANSACTION;", "CREATE TABLE t (k int);", "COMMIT;"}
	if got := fake.sent(); !slices.Equal(got, want) {
		t.Fatalf("sent to the database: %q", got)
	}
}

// The first refused batch ends the snapshot: nothing after it is sent, the
// error carries the batch, and the connection — with the open transaction —
// is closed instead of pooled.
func TestSQLServerSnapshotStopsAtTheFirstFailure(t *testing.T) {
	db, fake := openFake(t)
	fake.failOn = "nosuchtype"
	script := "BEGIN TRANSACTION;\nGO\n-- tables\nCREATE TABLE t (k nosuchtype);\nGO\nCOMMIT;\nGO\n"
	err := (sqlServerDriver{}).Snapshot(context.Background(), db, script)
	var batchErr *BatchError
	if !errors.As(err, &batchErr) {
		t.Fatalf("err = %v, want a *BatchError", err)
	}
	const refused = "-- tables\nCREATE TABLE t (k nosuchtype);"
	if batchErr.Index != 1 || batchErr.StartLine != 3 || batchErr.SQL != refused {
		t.Errorf("batch error = %+v", batchErr)
	}
	if err.Error() != "batch 2: fakedb: statement refused" {
		t.Errorf("message = %q", err)
	}
	if got := fake.sent(); !slices.Equal(got, []string{"BEGIN TRANSACTION;", refused, "CLOSE"}) {
		t.Fatalf("sent to the database: %q", got)
	}
}

func TestSQLServerDescribeError(t *testing.T) {
	ms := sqlServerDriver{}
	delta := "SET XACT_ABORT ON; SET QUOTED_IDENTIFIER ON;\n" +
		"MERGE INTO [person] AS t USING (SELECT N'Bob' AS [name]) AS s ON (t.[name] = s.[name]) WHEN NOT MATCHED THEN INSERT ([name]) VALUES (s.[name]);\n" +
		"   MERGE INTO [film] AS t USING (SELECT N'Bob' AS [title]) AS s ON (1 = 0) WHEN NOT MATCHED THEN INSERT ([title]) VALUES (s.[title]);\n"
	duplicate := mssql.Error{
		Number: 2627, State: 1, Class: 14, LineNo: 3,
		Message: "Violation of UNIQUE KEY constraint 'UQ__film__1'. Cannot insert duplicate key in object 'dbo.film'. The duplicate key value is (Bob).",
	}

	t.Run("a delta is one batch: the server's line is the script's", func(t *testing.T) {
		got := ms.DescribeError(delta, fmt.Errorf("delta 431 (data upsert): %w", duplicate))
		want := "delta 431 (data upsert): mssql: Violation of UNIQUE KEY constraint 'UQ__film__1'. " +
			"Cannot insert duplicate key in object 'dbo.film'. The duplicate key value is (Bob). (2627)\n" +
			"  number: 2627\n" +
			"  state: 1\n" +
			"  script line 3: MERGE INTO [film] AS t USING (SELECT N'Bob' AS [title]) AS s ON (1 = 0) " +
			"WHEN NOT MATCHED THEN INSERT ([title]) VALUES (s.[title]);"
		if got != want {
			t.Errorf("got:\n%s\nwant:\n%s", got, want)
		}
	})

	t.Run("a snapshot batch: the line is counted from the script's start", func(t *testing.T) {
		script := "BEGIN TRANSACTION;\nGO\n" + delta + "GO\nCOMMIT;\nGO\n"
		batch := splitBatches(script)[1]
		err := &BatchError{Index: 1, StartLine: batch.startLine, SQL: batch.sql, Err: duplicate}
		got := ms.DescribeError(script, err)
		if !strings.HasPrefix(got, "batch 2: mssql: Violation of UNIQUE KEY") ||
			!strings.Contains(got, "\n  script line 5: MERGE INTO [film] AS t") {
			t.Errorf("got:\n%s", got)
		}
	})

	t.Run("several errors for one statement: the first is the cause", func(t *testing.T) {
		cause := mssql.Error{Number: 5074, State: 1, LineNo: 2,
			Message: "The index 'idx_film_title' is dependent on column 'title'."}
		last := mssql.Error{Number: 4922, State: 9, LineNo: 2,
			Message: "ALTER TABLE ALTER COLUMN title failed because one or more objects access this column."}
		last.All = []mssql.Error{cause, last}
		got := ms.DescribeError("SET XACT_ABORT ON;\nALTER TABLE [film] ALTER COLUMN [title] NVARCHAR(50) NOT NULL;", last)
		want := "mssql: The index 'idx_film_title' is dependent on column 'title'. (5074)\n" +
			"mssql: ALTER TABLE ALTER COLUMN title failed because one or more objects access this column. (4922)\n" +
			"  number: 5074\n" +
			"  state: 1\n" +
			"  script line 2: ALTER TABLE [film] ALTER COLUMN [title] NVARCHAR(50) NOT NULL;"
		if got != want {
			t.Errorf("got:\n%s\nwant:\n%s", got, want)
		}
	})

	t.Run("an error the database did not send passes through", func(t *testing.T) {
		err := errors.New("delta 7 (data upsert): unexpected EOF")
		if got := ms.DescribeError("SELECT 1;", err); got != err.Error() {
			t.Errorf("got:\n%s", got)
		}
	})
}

// The server counts an error's line from the start of the innermost batch,
// so a statement run through EXEC(N'…') — every migration statement — reports
// line 1 wherever it stands. A line is named only when it is the one.
func TestLocateInBatch(t *testing.T) {
	const settings = "SET XACT_ABORT ON; SET QUOTED_IDENTIFIER ON;"
	const guard = "IF @@TRANCOUNT = 0 THROW 50002, N'EE-SNAPSHOT: an earlier batch failed', 1;"
	const index = "EXEC(N'CREATE UNIQUE INDEX [uq_film_k] ON [film] ([k]);');"
	const column = "EXEC(N'ALTER TABLE [film] ADD [year] INT NULL;');"
	const merge = "MERGE INTO [film] AS t USING (SELECT 1 AS [k]) AS s ON (1 = 0) WHEN NOT MATCHED THEN INSERT ([k]) VALUES (s.[k]);"
	cases := []struct {
		name     string
		batch    string
		lineNo   int32
		procName string
		wantLine int // 0 = no line named
		wantText string
	}{
		{name: "a line of the batch", batch: settings + "\n" + merge + "\n" + merge, lineNo: 3,
			wantLine: 3, wantText: merge},
		{name: "line 1 of a batch without dynamic SQL", batch: guard + "\n" + merge, lineNo: 1,
			wantLine: 1, wantText: guard},
		{name: "line 1 with one EXEC below the settings: the EXEC raised it",
			batch: settings + "\n-- migrate film\n" + index, lineNo: 1, wantLine: 3, wantText: index},
		{name: "line 1 when the EXEC is the first line", batch: index, lineNo: 1,
			wantLine: 1, wantText: index},
		{name: "line 1 with two EXECs: either may have raised it",
			batch: settings + "\n" + column + "\n" + index, lineNo: 1},
		{name: "line 1 with an EXEC below a line that can raise",
			batch: guard + "\n" + index, lineNo: 1},
		{name: "sp_executesql is dynamic SQL too",
			batch: settings + "\nEXEC sp_executesql N'SELECT 1/0';\n" + index, lineNo: 1},
		{name: "a line beyond the EXEC lines is the batch's own",
			batch: settings + "\n" + column + "\n" + merge, lineNo: 3, wantLine: 3, wantText: merge},
		{name: "an error raised inside a procedure names a line of the procedure",
			batch: settings + "\nEXEC sp_rename N'[film].[a]', N'b', N'COLUMN';", lineNo: 2, procName: "sp_rename"},
		{name: "line 1 when nothing on it can raise", batch: settings + "\n" + merge, lineNo: 1},
		{name: "no line reported", batch: merge, lineNo: 0},
		{name: "a line the batch does not have", batch: merge, lineNo: 7},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			line, text, ok := locateInBatch(tc.batch, mssql.Error{LineNo: tc.lineNo, ProcName: tc.procName})
			if ok != (tc.wantLine != 0) || line != tc.wantLine || text != tc.wantText {
				t.Errorf("got line %d %q (ok=%v), want line %d %q", line, text, ok, tc.wantLine, tc.wantText)
			}
		})
	}
}

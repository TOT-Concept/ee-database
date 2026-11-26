package apply

import "testing"

// The statements the server renders for SQL Server (one line each, as they
// are sent).
func TestClassifySQLServer(t *testing.T) {
	cases := []struct {
		name  string
		stmt  string
		kind  opKind
		table string
	}{
		{
			name: "row upsert",
			stmt: `MERGE INTO [film] AS t USING (SELECT N'L''Été' AS [title], N'{"en": "Hot; dry"}' AS [tagline], 42 AS [_sync_revision]) AS s ON (t.[title] IS NOT DISTINCT FROM s.[title]) WHEN MATCHED AND t.[_sync_revision] < s.[_sync_revision] AND (t.[tagline] IS DISTINCT FROM s.[tagline]) THEN UPDATE SET [tagline] = s.[tagline], [_sync_revision] = s.[_sync_revision], [_updated_at] = SYSDATETIMEOFFSET() WHEN NOT MATCHED THEN INSERT ([title], [tagline], [_sync_revision]) VALUES (s.[title], s.[tagline], s.[_sync_revision])`,
			kind: opUpsert, table: "film",
		},
		{
			name: "keyless insert",
			stmt: `INSERT INTO [film_notes] ([_film_id], [value]) VALUES ((SELECT [_id] FROM [film] WHERE [title] = N'Heat'), N'a note')`,
			kind: opInsert, table: "film_notes",
		},
		{
			name: "reconciliation prune",
			stmt: `DELETE FROM [film_genres] WHERE [_film_id] = (SELECT [_id] FROM [film] WHERE [title] = N'Heat') AND NOT EXISTS (SELECT 1 FROM (VALUES (N'drama'), (N'comedy')) AS k([value]) WHERE k.[value] = [film_genres].[value]) AND [_sync_revision] < 42`,
			kind: opPrune, table: "film_genres",
		},
		{
			name: "tombstone delete",
			stmt: `DELETE FROM [film] WHERE [title] = N'Heat'`,
			kind: opDelete, table: "film",
		},
		{
			name: "wholesale wipe",
			stmt: `DELETE FROM [film_notes]`,
			kind: opWipe, table: "film_notes",
		},
		{
			// The words of a value are not the statement's: a national
			// literal is stepped over like a plain one.
			name: "keywords inside a national literal",
			stmt: `INSERT INTO [quote] ([text]) VALUES (N'DELETE FROM everything WHERE (it hurts')`,
			kind: opInsert, table: "quote",
		},
		{
			name: "detached links name their table, not the alias",
			stmt: `DELETE r FROM [person_scenes_links] AS r WHERE EXISTS (SELECT 1 FROM [scene] WHERE [scene].[_id] = r.[_target_id] AND [scene].[number] = 3)`,
			kind: opDelete, table: "person_scenes_links",
		},
		{
			name: "detached reference names its table, not the alias",
			stmt: `UPDATE r SET [prop_id] = NULL FROM [note] AS r WHERE EXISTS (SELECT 1 FROM [scene_props] WHERE [scene_props].[_id] = r.[prop_id])`,
			kind: opUpdate, table: "note",
		},
		{
			name: "update joined to its parent",
			stmt: `UPDATE d SET d.[_lighthouse_registry_id] = p.[registry_id] FROM [lighthouse_aliases] AS d JOIN [lighthouse] AS p ON d.[_lighthouse_name] = p.[name]`,
			kind: opUpdate, table: "lighthouse_aliases",
		},
		{
			name: "plain update",
			stmt: `UPDATE [film] SET [value_key] = JSON_VALUE([value], N'$."fr"') WHERE [value_key] IS NULL`,
			kind: opUpdate, table: "film",
		},
		{
			name: "guarded create table",
			stmt: `IF OBJECT_ID(N'film', N'U') IS NULL CREATE TABLE [film] ([_id] BIGINT IDENTITY(1,1) PRIMARY KEY, [title] NVARCHAR(400) COLLATE Latin1_General_100_BIN2 NOT NULL, UNIQUE ([title]))`,
			kind: opDDL, table: "film",
		},
		{
			name: "guarded add column",
			stmt: `IF COL_LENGTH(N'film', N'year') IS NULL ALTER TABLE [film] ADD [year] INT NULL`,
			kind: opDDL, table: "film",
		},
		{
			name: "guarded index reports its table",
			stmt: `IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_film_rating' AND object_id = OBJECT_ID(N'film')) CREATE INDEX [idx_film_rating] ON [film] ([rating])`,
			kind: opDDL, table: "film",
		},
		{
			name: "guarded foreign key",
			stmt: `IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'fk_film_director_id' AND parent_object_id = OBJECT_ID(N'film')) AND OBJECT_ID(N'film', N'U') IS NOT NULL ALTER TABLE [film] ADD CONSTRAINT [fk_film_director_id] FOREIGN KEY ([director_id]) REFERENCES [person] ([_id])`,
			kind: opDDL, table: "film",
		},
		{
			name: "drop constraint",
			stmt: `ALTER TABLE [film] DROP CONSTRAINT IF EXISTS [nn_film_rating]`,
			kind: opDDL, table: "film",
		},
		{
			name: "a schema qualifier and a name with a space, a dot and a bracket",
			stmt: `MERGE INTO [dbo].[my.odd ]] table] AS t USING (SELECT 1 AS [k]) AS s ON (t.[k] = s.[k]) WHEN NOT MATCHED THEN INSERT ([k]) VALUES (s.[k])`,
			kind: opUpsert, table: "my.odd ] table",
		},
		{
			// A migration statement travels as EXEC(N'…'): reporting the
			// statement it guards is the whole point of the line.
			name: "guarded unique index inside EXEC",
			stmt: `EXEC(N'IF OBJECT_ID(N''film'', N''U'') IS NULL RETURN; IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = N''uq_film_call_sign'' AND object_id = OBJECT_ID(N''film'')) RETURN; DECLARE @n BIGINT = (SELECT COUNT(*) FROM (SELECT 1 AS one FROM [film] WHERE [call_sign] IS NOT NULL GROUP BY [call_sign] HAVING COUNT(*) > 1) AS dup); IF @n > 0 BEGIN DECLARE @m NVARCHAR(2048) = CONCAT(N''EE-MIGRATE: '', @n, N'' duplicate value group(s) in film — deduplicate these rows, then re-drive this statement to create the constraint''); THROW 50001, @m, 1; END; CREATE UNIQUE INDEX [uq_film_call_sign] ON [film] ([call_sign]) WHERE [call_sign] IS NOT NULL;')`,
			kind: opDDL, table: "film",
		},
		{
			name: "computed column inside EXEC",
			stmt: `EXEC(N'ALTER TABLE [film_taglines] ADD [value_zh-CN] AS CAST(JSON_VALUE([value], N''$."zh-CN"'') AS NVARCHAR(400));')`,
			kind: opDDL, table: "film_taglines",
		},
		{
			// The check constraint is added by an EXEC inside the EXEC.
			name: "not null check, two levels down",
			stmt: `EXEC(N'IF OBJECT_ID(N''film'', N''U'') IS NULL RETURN; DECLARE @bad NVARCHAR(MAX) = N'''', @n BIGINT; IF COL_LENGTH(N''film'', N''rating'') IS NOT NULL BEGIN EXEC sp_executesql N''SELECT @n = COUNT(*) FROM [film] WHERE [rating] IS NULL'', N''@n BIGINT OUTPUT'', @n = @n OUTPUT; IF @n > 0 SET @bad += CONCAT(N''rating ('', @n, N'' row(s)), ''); END; IF @bad <> N'''' BEGIN DECLARE @m NVARCHAR(2048) = CONCAT(N''EE-MIGRATE: NULL values in film block NOT NULL on '', LEFT(@bad, LEN(@bad) - 1)); THROW 50001, @m, 1; END; IF COL_LENGTH(N''film'', N''rating'') IS NOT NULL EXEC(N''ALTER TABLE [film] ADD CONSTRAINT [nn_film_rating] CHECK ([rating] IS NOT NULL)'');')`,
			kind: opDDL, table: "film",
		},
		{
			name: "ordinal backfill through a CTE names the table it reads",
			stmt: `EXEC(N'WITH s AS (SELECT [_ordinal], ROW_NUMBER() OVER (PARTITION BY [_mission_id] ORDER BY [_id]) - 1 AS rn FROM [mission_steps]) UPDATE s SET [_ordinal] = rn WHERE [_ordinal] IS DISTINCT FROM rn;')`,
			kind: opUpdate, table: "mission_steps",
		},
		{
			name: "duplicate collapse through a CTE",
			stmt: `EXEC(N'WITH s AS (SELECT ROW_NUMBER() OVER (PARTITION BY [_film_id], [value] ORDER BY [_id]) AS rn FROM [film_tags]) DELETE FROM s WHERE rn > 1;')`,
			kind: opDelete, table: "film_tags",
		},
		{
			// A statement assembled at run time has no table to read.
			name: "index drop built from the catalog",
			stmt: `EXEC(N'DECLARE @s NVARCHAR(MAX) = (SELECT N''DROP INDEX [idx_film_rating] ON '' + QUOTENAME(OBJECT_NAME(object_id)) FROM sys.indexes WHERE name = N''idx_film_rating''); IF @s IS NOT NULL EXEC(@s);')`,
			kind: opOther, table: "",
		},
		{
			name: "procedure call",
			stmt: `EXEC sp_rename N'[film].[old]', N'new', N'COLUMN'`,
			kind: opOther, table: "",
		},
		{
			name: "snapshot step guard",
			stmt: `IF @@TRANCOUNT = 0 OR CAST(SESSION_CONTEXT(N'ee_snapshot') AS INT) <> 1 THROW 50002, N'EE-SNAPSHOT: an earlier batch failed — nothing was applied', 1`,
			kind: opOther, table: "",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			kind, table := classify(c.stmt)
			if kind != c.kind || table != c.table {
				t.Fatalf("got (%v, %q), want (%v, %q)", kind, table, c.kind, c.table)
			}
		})
	}
}

func TestIdent(t *testing.T) {
	cases := map[string]string{
		`"aircraft_family"`: "aircraft_family",
		`public."t"`:        "t",
		`"my.table"`:        "my.table",
		`[film]`:            "film",
		`[dbo].[film]`:      "film",
		`dbo.[film]`:        "film",
		`[my.table]`:        "my.table",
		`[a]]b]`:            "a]b",
		`film`:              "film",
	}
	for tok, want := range cases {
		if got := ident(tok); got != want {
			t.Errorf("ident(%s) = %q, want %q", tok, got, want)
		}
	}
}

// One entity's delta as SQL Server receives it: the settings line every unit
// opens with is not a statement of the write.
func TestSummarizeSQLServerEntityUpsert(t *testing.T) {
	script := `SET XACT_ABORT ON; SET QUOTED_IDENTIFIER ON;
MERGE INTO [aircraft_family] AS t USING (SELECT N'E-Jet' AS [k], 7 AS [_sync_revision]) AS s ON (t.[k] IS NOT DISTINCT FROM s.[k]) WHEN MATCHED AND t.[_sync_revision] < s.[_sync_revision] THEN UPDATE SET [_sync_revision] = s.[_sync_revision] WHEN NOT MATCHED THEN INSERT ([k], [_sync_revision]) VALUES (s.[k], s.[_sync_revision]);
MERGE INTO [aircraft_variant] AS t USING (SELECT (SELECT [_id] FROM [aircraft_family] WHERE [k] = N'E-Jet') AS [_aircraft_family_id], N'E170' AS [k], 7 AS [_sync_revision]) AS s ON (t.[_aircraft_family_id] IS NOT DISTINCT FROM s.[_aircraft_family_id] AND t.[k] IS NOT DISTINCT FROM s.[k]) WHEN MATCHED AND t.[_sync_revision] < s.[_sync_revision] THEN UPDATE SET [_sync_revision] = s.[_sync_revision] WHEN NOT MATCHED THEN INSERT ([_aircraft_family_id], [k], [_sync_revision]) VALUES (s.[_aircraft_family_id], s.[k], s.[_sync_revision]);
MERGE INTO [aircraft_variant] AS t USING (SELECT (SELECT [_id] FROM [aircraft_family] WHERE [k] = N'E-Jet') AS [_aircraft_family_id], N'E175' AS [k], 7 AS [_sync_revision]) AS s ON (t.[_aircraft_family_id] IS NOT DISTINCT FROM s.[_aircraft_family_id] AND t.[k] IS NOT DISTINCT FROM s.[k]) WHEN MATCHED AND t.[_sync_revision] < s.[_sync_revision] THEN UPDATE SET [_sync_revision] = s.[_sync_revision] WHEN NOT MATCHED THEN INSERT ([_aircraft_family_id], [k], [_sync_revision]) VALUES (s.[_aircraft_family_id], s.[k], s.[_sync_revision]);
DELETE FROM [aircraft_variant] WHERE [_aircraft_family_id] = (SELECT [_id] FROM [aircraft_family] WHERE [k] = N'E-Jet') AND NOT EXISTS (SELECT 1 FROM (VALUES (N'E170'), (N'E175')) AS k([k]) WHERE k.[k] = [aircraft_variant].[k]) AND [_sync_revision] < 7;
`
	s := Summarize(script)
	got := s.Summary(0)
	want := "4 statement(s): aircraft_family 1 upsert, aircraft_variant 2 upserts + 1 prune"
	if got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
}

// A SQL Server snapshot is cut into batches by GO lines; the shape reads
// through them, and a GO inside a value stays a value.
func TestSummarizeSQLServerSnapshot(t *testing.T) {
	script := "-- Entity Enricher snapshot (dialect sqlserver)\r\n" +
		"SET XACT_ABORT ON; SET QUOTED_IDENTIFIER ON; SET ANSI_NULLS ON;\r\n" +
		"EXEC sp_set_session_context N'ee_snapshot', 0;\r\n" +
		"BEGIN TRANSACTION;\r\n" +
		"GO\r\n" +
		"EXEC sp_set_session_context N'ee_snapshot', 1;\n" +
		"IF OBJECT_ID(N'album', N'U') IS NULL CREATE TABLE [album] ([k] NVARCHAR(400) NOT NULL, [_sync_revision] BIGINT NOT NULL DEFAULT 0, UNIQUE ([k]))\n" +
		"  go  \n" +
		"MERGE INTO [album] AS t USING (SELECT N'Meddle\nGO\nWest' AS [k], 1 AS [_sync_revision]) AS s ON (t.[k] IS NOT DISTINCT FROM s.[k]) WHEN NOT MATCHED THEN INSERT ([k], [_sync_revision]) VALUES (s.[k], s.[_sync_revision]);\n" +
		"GO\n" +
		"COMMIT;\n" +
		"GO\n"
	s := Summarize(script)
	// BEGIN TRANSACTION, the CREATE (it ends at its GO line, with no
	// semicolon), the MERGE and COMMIT.
	if got, want := s.Summary(0), "4 statement(s): album 1 upsert + 1 ddl"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
}

// PostgreSQL has session statements of its own; only the three-word T-SQL
// switch is left out of the count.
func TestSessionSetupIsTheTSQLFormOnly(t *testing.T) {
	skipped := []string{
		"SET XACT_ABORT ON", "set quoted_identifier on", "SET ANSI_NULLS OFF",
		"-- a header line\nSET XACT_ABORT ON",
		"EXEC sp_set_session_context N'ee_snapshot', 3",
	}
	for _, stmt := range skipped {
		if !sessionSetup(stmt) {
			t.Errorf("%q must not count as a statement", stmt)
		}
	}
	counted := []string{
		"SET search_path = public", "SET standard_conforming_strings = on",
		"SET CONSTRAINTS ALL DEFERRED", "EXEC sp_rename N'[a]', N'b'", "EXEC(N'SET XACT_ABORT ON')",
		"BEGIN TRANSACTION", "COMMIT",
	}
	for _, stmt := range counted {
		if sessionSetup(stmt) {
			t.Errorf("%q is a statement", stmt)
		}
	}
}

package apply

import (
	"strings"
	"testing"
)

func TestSplitStatementsStepsOverQuotingAndComments(t *testing.T) {
	script := `
-- a comment with a ; semicolon
INSERT INTO "t" ("a") VALUES ('x; y');
/* block ; comment /* nested ; */ still */
DELETE FROM "u" WHERE "a" = '''quoted''; not an end';
DO $$ BEGIN PERFORM 1; PERFORM 2; END $$;
`
	got := splitStatements(script)
	if len(got) != 3 {
		t.Fatalf("expected 3 statements, got %d: %#v", len(got), got)
	}
	// A leading comment rides along with its statement; classification looks
	// past it, which is what the summary actually depends on.
	for i, want := range []opKind{opInsert, opDelete, opOther} {
		if kind, _ := classify(got[i]); kind != want {
			t.Fatalf("statement %d classified %v, want %v (%q)", i, kind, want, got[i])
		}
	}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		name  string
		stmt  string
		kind  opKind
		table string
	}{
		{
			name: "row upsert",
			stmt: `INSERT INTO "aircraft_variant" ("a", "b") VALUES (1, 2)
ON CONFLICT ("a") DO UPDATE SET "b" = EXCLUDED."b" WHERE true`,
			kind: opUpsert, table: "aircraft_variant",
		},
		{
			name: "plain insert",
			stmt: `INSERT INTO "t" ("a") VALUES (1)`,
			kind: opInsert, table: "t",
		},
		{
			// The revision guard is what marks a reconciliation cleanup — a
			// tombstone (next case) is the same statement without it.
			name: "reconciliation prune",
			stmt: `DELETE FROM "engine_option" WHERE "_parent_id" = 1
AND ("k") NOT IN (('a')) AND "_sync_revision" < 42`,
			kind: opPrune, table: "engine_option",
		},
		{
			name: "tombstone delete",
			stmt: `DELETE FROM "vinyl_record" WHERE "id" = 'abc'`,
			kind: opDelete, table: "vinyl_record",
		},
		{
			name: "wholesale wipe",
			stmt: `DELETE FROM "child"`,
			kind: opWipe, table: "child",
		},
		{
			// A value containing the word must not upgrade a wipe to a prune.
			name: "where inside a literal is not a where clause",
			stmt: `DELETE FROM "child" /* rebuild */`,
			kind: opWipe, table: "child",
		},
		{
			name: "create table",
			stmt: `CREATE TABLE IF NOT EXISTS "aircraft_family" ("_id" BIGSERIAL PRIMARY KEY)`,
			kind: opDDL, table: "aircraft_family",
		},
		{
			name: "create index reports its table",
			stmt: `CREATE UNIQUE INDEX IF NOT EXISTS "ix_a" ON "aircraft_family" ("k")`,
			kind: opDDL, table: "aircraft_family",
		},
		{
			name: "alter table",
			stmt: `ALTER TABLE "aircraft_family" ADD COLUMN IF NOT EXISTS "x" TEXT`,
			kind: opDDL, table: "aircraft_family",
		},
		{
			name: "schema qualifier is dropped",
			stmt: `INSERT INTO public."t" ("a") VALUES (1) ON CONFLICT DO NOTHING`,
			kind: opUpsert, table: "t",
		},
		{
			// The projection guards migration DDL in a DO block; reporting the
			// statement it guards is the whole point of the line.
			name: "guarded fk add reports its table",
			stmt: `DO $ee_mig$ BEGIN ALTER TABLE "vinyl_record" ADD CONSTRAINT "fk_x"
FOREIGN KEY ("producteur_id") REFERENCES "person" ("id") DEFERRABLE INITIALLY DEFERRED;
EXCEPTION WHEN duplicate_object THEN NULL; END $ee_mig$`,
			kind: opDDL, table: "vinyl_record",
		},
		{
			name: "guarded index behind an extension probe",
			stmt: `DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm')
THEN CREATE INDEX IF NOT EXISTS "ix_t" ON "track" USING gin ("k" gin_trgm_ops); END IF; END $$`,
			kind: opDDL, table: "track",
		},
		{
			name: "no guarded action at all",
			stmt: `DO $$ BEGIN PERFORM 1; END $$`,
			kind: opOther, table: "",
		},
		{
			// The NOT NULL block builds its statements at run time (EXECUTE
			// format(…)), so it names no table — and its ARRAY[…]::text[]
			// must not be read as a T-SQL bracket name that swallows the rest.
			name: "guarded not null block names no table",
			stmt: `DO $ee_mig$
DECLARE c TEXT; n BIGINT; bad TEXT := '';
BEGIN
    IF to_regclass('film') IS NULL THEN RETURN; END IF;
    FOREACH c IN ARRAY ARRAY['rating', 'year']::text[] LOOP
        IF EXISTS (SELECT 1 FROM pg_attribute
                   WHERE attrelid = to_regclass('film')
                     AND attname = c AND attnum > 0
                     AND NOT attisdropped AND NOT attnotnull) THEN
            EXECUTE format('SELECT count(*) FROM %I WHERE %I IS NULL',
                           'film', c) INTO n;
            IF n > 0 THEN
                bad := bad || format('%s (%s row(s)), ', c, n);
            END IF;
        END IF;
    END LOOP;
    IF bad <> '' THEN
        RAISE EXCEPTION 'EE-MIGRATE: NULL values in film block NOT NULL on %', left(bad, -2);
    END IF;
    FOREACH c IN ARRAY ARRAY['rating', 'year']::text[] LOOP
        IF EXISTS (SELECT 1 FROM pg_attribute
                   WHERE attrelid = to_regclass('film')
                     AND attname = c AND attnum > 0
                     AND NOT attisdropped AND NOT attnotnull) THEN
            EXECUTE format('ALTER TABLE %I ALTER COLUMN %I SET NOT NULL',
                           'film', c);
        END IF;
    END LOOP;
END $ee_mig$`,
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

// One entity's rendered upsert: the shape line is what answers "what does ONE
// entity cost my replica" without archaeology on acked deltas.
func TestSummarizeEntityUpsert(t *testing.T) {
	script := `
INSERT INTO "aircraft_family" ("k", "_sync_revision") VALUES ('E-Jet', 7)
ON CONFLICT ("k") DO UPDATE SET "_sync_revision" = 7;
INSERT INTO "aircraft_variant" ("_aircraft_family_k", "k") VALUES ('E-Jet', 'E170')
ON CONFLICT ("_aircraft_family_k", "k") DO UPDATE SET "k" = EXCLUDED."k";
INSERT INTO "aircraft_variant" ("_aircraft_family_k", "k") VALUES ('E-Jet', 'E175')
ON CONFLICT ("_aircraft_family_k", "k") DO UPDATE SET "k" = EXCLUDED."k";
DELETE FROM "aircraft_variant" WHERE "_aircraft_family_k" = 'E-Jet'
AND ("k") NOT IN (('E170'), ('E175')) AND "_sync_revision" < 7;
`
	s := Summarize(script)
	if s.Statements != 4 {
		t.Fatalf("statements = %d, want 4", s.Statements)
	}
	got := s.Summary(0)
	want := "4 statement(s): aircraft_family 1 upsert, aircraft_variant 2 upserts + 1 prune"
	if got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	// Emission order (target before referrer) is what the line reads in.
	if tables := s.Tables(); tables[0].Table != "aircraft_family" {
		t.Fatalf("emission order lost: %q first", tables[0].Table)
	}
}

func TestShapeMergeAndCap(t *testing.T) {
	var total Shape
	for i := 0; i < 3; i++ {
		total.Merge(Summarize(`INSERT INTO "a" ("k") VALUES (1) ON CONFLICT ("k") DO UPDATE SET "k" = 1;
INSERT INTO "b" ("k") VALUES (1);`))
	}
	if total.Statements != 6 {
		t.Fatalf("statements = %d, want 6", total.Statements)
	}
	if got := total.Summary(0); got != "6 statement(s): a 3 upserts, b 3 inserts" {
		t.Fatalf("summary = %q", got)
	}

	var wide Shape
	for _, table := range []string{"t1", "t2", "t3", "t4"} {
		wide.Merge(Summarize(`INSERT INTO "` + table + `" ("k") VALUES (1);`))
	}
	wide.Merge(Summarize(`INSERT INTO "t1" ("k") VALUES (2);`))
	got := wide.Summary(2)
	// t1 (2 writes) survives on volume; the second slot goes to the first of
	// the ties, and the rest collapse into the count.
	if !strings.Contains(got, "t1 2 inserts") || !strings.Contains(got, "(+2 more table(s))") {
		t.Fatalf("capped summary = %q", got)
	}
}

func TestSummaryWithoutTables(t *testing.T) {
	s := Summarize(`SET search_path = public; DO $$ BEGIN PERFORM 1; END $$;`)
	if got := s.Summary(6); got != "2 statement(s)" {
		t.Fatalf("summary = %q, want %q", got, "2 statement(s)")
	}
}

package apply

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/TOT-Concept/ee-database/internal/framing"
)

// Batch's report is what the apply log states about a window; run it against a
// real (sqlite) database so the shape roll-up, the cursor and the failure path
// are proven together rather than by inspection.
func TestBatchReportsTheWriteShape(t *testing.T) {
	db, err := Open("sqlite", filepath.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
CREATE TABLE "album" ("k" TEXT PRIMARY KEY, "_sync_revision" INTEGER);
CREATE TABLE "track" ("_album_k" TEXT, "k" TEXT, "_sync_revision" INTEGER,
                      PRIMARY KEY ("_album_k", "k"));`); err != nil {
		t.Fatal(err)
	}

	entityType := "Album"
	deltas := []framing.Delta{{
		ID: 41, Kind: "schema", Op: "migrate",
		SQL: `CREATE INDEX IF NOT EXISTS "ix_track_k" ON "track" ("k");`,
	}, {
		ID: 42, Kind: "data", Op: "upsert", EntityType: &entityType,
		SQL: `INSERT INTO "album" ("k", "_sync_revision") VALUES ('Meddle', 7)
ON CONFLICT ("k") DO UPDATE SET "_sync_revision" = 7;
INSERT INTO "track" ("_album_k", "k", "_sync_revision") VALUES ('Meddle', 'Echoes', 7)
ON CONFLICT ("_album_k", "k") DO UPDATE SET "_sync_revision" = 7;
DELETE FROM "track" WHERE "_album_k" = 'Meddle' AND "k" NOT IN ('Echoes')
AND "_sync_revision" < 7;`,
	}}

	res, err := Batch(ctx, db, deltas, true)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if res.LastID != 42 || res.FailedID != nil {
		t.Fatalf("cursor = %d / failed = %v", res.LastID, res.FailedID)
	}
	if got := res.Shape.Summary(0); got != "4 statement(s): track 1 upsert + 1 prune + 1 ddl, album 1 upsert" {
		t.Fatalf("batch shape = %q", got)
	}
	if len(res.Deltas) != 2 {
		t.Fatalf("per-delta detail = %d entries, want 2", len(res.Deltas))
	}
	if res.Deltas[1].EntityType != "Album" || res.Deltas[1].Op != "upsert" {
		t.Fatalf("delta detail lost its identity: %+v", res.Deltas[1])
	}
	if got := res.Deltas[1].Shape.Summary(0); got != "3 statement(s): album 1 upsert, track 1 upsert + 1 prune" {
		t.Fatalf("delta shape = %q", got)
	}
	if res.Elapsed <= 0 {
		t.Fatal("batch reported no wall time")
	}

	// Without detail the window still reports its shape — that line is the
	// only lasting record once the deltas are acked and purged.
	res, err = Batch(ctx, db, deltas, false)
	if err != nil {
		t.Fatalf("re-apply (idempotent): %v", err)
	}
	if len(res.Deltas) != 0 || res.Shape.Statements != 4 {
		t.Fatalf("quiet mode: %d detail entries, %d statements", len(res.Deltas), res.Shape.Statements)
	}
}

func TestBatchRollsBackAndNamesTheFailingDelta(t *testing.T) {
	db, err := Open("sqlite", filepath.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE "album" ("k" TEXT PRIMARY KEY);`); err != nil {
		t.Fatal(err)
	}
	res, err := Batch(ctx, db, []framing.Delta{
		{ID: 1, Kind: "data", Op: "upsert", SQL: `INSERT INTO "album" ("k") VALUES ('a');`},
		{ID: 2, Kind: "data", Op: "upsert", SQL: `INSERT INTO "nope" ("k") VALUES ('b');`},
	}, false)
	if err == nil {
		t.Fatal("expected the missing table to fail the batch")
	}
	if res.FailedID == nil || *res.FailedID != 2 {
		t.Fatalf("failed delta = %v, want 2", res.FailedID)
	}
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM "album"`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("rollback left %d row(s) behind", n)
	}
}

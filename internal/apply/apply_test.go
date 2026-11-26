package apply

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/TOT-Concept/ee-database/internal/framing"
)

// Batch's report is what the apply log states about a window: the shape
// roll-up, the cursor and the transaction around the deltas are proven
// together on the recording driver (the rollback itself is proven against a
// real database in the live tests).
func TestBatchReportsTheWriteShape(t *testing.T) {
	db, fake := openFake(t)
	ctx := context.Background()

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
	// One window = one transaction, one Exec per delta (its whole script).
	want := []string{"BEGIN", deltas[0].SQL, deltas[1].SQL, "COMMIT"}
	if got := fake.sent(); !slices.Equal(got, want) {
		t.Fatalf("sent to the database:\n%q\nwant:\n%q", got, want)
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
		t.Fatalf("re-apply: %v", err)
	}
	if len(res.Deltas) != 0 || res.Shape.Statements != 4 {
		t.Fatalf("quiet mode: %d detail entries, %d statements", len(res.Deltas), res.Shape.Statements)
	}
}

func TestBatchRollsBackAndNamesTheFailingDelta(t *testing.T) {
	db, fake := openFake(t)
	fake.failOn = `"nope"`
	deltas := []framing.Delta{
		{ID: 1, Kind: "data", Op: "upsert", SQL: `INSERT INTO "album" ("k") VALUES ('a');`},
		{ID: 2, Kind: "data", Op: "upsert", SQL: `INSERT INTO "nope" ("k") VALUES ('b');`},
		{ID: 3, Kind: "data", Op: "upsert", SQL: `INSERT INTO "album" ("k") VALUES ('c');`},
	}
	res, err := Batch(context.Background(), db, deltas, false)
	if err == nil {
		t.Fatal("expected the refused statement to fail the batch")
	}
	if !strings.HasPrefix(err.Error(), "delta 2 (data upsert): ") {
		t.Fatalf("error does not name the delta: %v", err)
	}
	if res.FailedID == nil || *res.FailedID != 2 {
		t.Fatalf("failed delta = %v, want 2", res.FailedID)
	}
	// Nothing after the failure is sent, and the window is rolled back whole.
	want := []string{"BEGIN", deltas[0].SQL, deltas[1].SQL, "ROLLBACK"}
	if got := fake.sent(); !slices.Equal(got, want) {
		t.Fatalf("sent to the database:\n%q\nwant:\n%q", got, want)
	}
}

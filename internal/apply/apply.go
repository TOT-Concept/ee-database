// Package apply executes delta SQL against the user's own database.
//
// Deltas carry ready-to-run multi-statement SQL rendered server-side for the
// registration's dialect; every mapped table has a _sync_revision guard, so
// re-applying a delta is physically harmless. What differs between target
// database types sits behind Driver (one file per dialect); Batch and the
// apply-log shape (Summarize) are written once.
package apply

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/TOT-Concept/ee-database/internal/framing"
)

// DeltaResult is one applied delta's identity, write shape and wall time.
// Only filled when the caller asks for per-delta detail (--verbose).
type DeltaResult struct {
	ID         int64
	Kind       string
	Op         string
	EntityType string
	Shape      Shape
	Elapsed    time.Duration
}

// BatchResult reports what one applied window did to the replica: how long the
// transaction took and, per table, which statement shapes it ran. The window is
// the transaction — the server never splits an enrichment across two — so this
// is also the answer to "how do deltas group into transactions".
type BatchResult struct {
	LastID   int64
	FailedID *int64
	Elapsed  time.Duration
	Shape    Shape
	Deltas   []DeltaResult
}

// Batch applies deltas in id order inside one transaction.
//
// On failure it rolls back and returns the offending delta's id — nothing from
// the batch is committed, the server's lease expires, and the same window is
// redelivered after the cause is fixed.
//
// detail additionally keeps each delta's own shape and timing; the aggregate
// shape is always computed (one pass over SQL the transaction is about to run
// costs nothing next to running it) so the apply log can state the write shape
// instead of a delta count — the deltas are gone once acked.
func Batch(ctx context.Context, db *sql.DB, deltas []framing.Delta, detail bool) (BatchResult, error) {
	res := BatchResult{}
	started := time.Now()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return res, fmt.Errorf("begin: %w", err)
	}
	if detail {
		res.Deltas = make([]DeltaResult, 0, len(deltas))
	}
	for i := range deltas {
		d := deltas[i]
		shape := Summarize(d.SQL)
		stmtStart := time.Now()
		if _, err := tx.ExecContext(ctx, d.SQL); err != nil {
			_ = tx.Rollback()
			res.FailedID = &d.ID
			res.Elapsed = time.Since(started)
			return res, fmt.Errorf("delta %d (%s %s): %w", d.ID, d.Kind, d.Op, err)
		}
		res.Shape.Merge(shape)
		if detail {
			entityType := ""
			if d.EntityType != nil {
				entityType = *d.EntityType
			}
			res.Deltas = append(res.Deltas, DeltaResult{
				ID: d.ID, Kind: d.Kind, Op: d.Op, EntityType: entityType,
				Shape: shape, Elapsed: time.Since(stmtStart),
			})
		}
		res.LastID = d.ID
	}
	if err := tx.Commit(); err != nil {
		res.Elapsed = time.Since(started)
		return res, fmt.Errorf("commit: %w", err)
	}
	res.Elapsed = time.Since(started)
	return res, nil
}

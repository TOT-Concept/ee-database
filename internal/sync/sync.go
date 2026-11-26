// Package sync runs the data-plane loop: dial the WebSocket, receive delta
// batches, apply them to the target database, acknowledge.
package sync

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/TOT-Concept/ee-database/internal/apply"
	"github.com/TOT-Concept/ee-database/internal/auth"
	"github.com/TOT-Concept/ee-database/internal/framing"
)

const (
	pingInterval = 30 * time.Second
	// Batches carry rendered SQL for up to page_limit deltas.
	maxReadBytes = 64 * 1024 * 1024
)

// Version is the CLI release version — single source for the version command,
// run banners and the wire agent identity below.
const Version = "1.7.0"

// Agent identifies this client in User-Agent and hello frames.
const Agent = "ee-database/" + Version

// TrigramAdvisor warns — once per target, never blocking — when the feed
// actually ships pg_trgm search indexes at a replica that lacks the extension.
// The DDL is self-guarded server-side (a DO block gated on pg_extension), so
// those statements silently no-op; this advisory is what tells the operator
// WHY the index is not there and how to get it. Content-gated on the SQL so a
// replica whose schemas carry no `search` intent never sees the message
// (host mode additionally warns unconditionally at claim time, since its
// assigned registrations change over time).
type TrigramAdvisor struct {
	Missing bool
	warned  bool
}

// Check scans one SQL script and emits the advisory on the first sight of a
// trigram index. Nil-safe so callers without a report can pass nil through.
func (a *TrigramAdvisor) Check(sqlText string, logger *log.Logger) {
	if a == nil || !a.Missing || a.warned || !strings.Contains(sqlText, "gin_trgm_ops") {
		return
	}
	a.warned = true
	logger.Printf("warning: this sync ships database search indexes but the pg_trgm " +
		"extension is not installed — they are skipped (no-op) until a database owner " +
		"runs 'CREATE EXTENSION pg_trgm;' on this replica; data and all other DDL " +
		"apply normally, and the indexes materialize on the next publish or reconcile")
}

// Options are the per-target knobs Bootstrap and Pump share for one run.
type Options struct {
	// Driver is the target database type: it applies the snapshot and
	// describes apply errors.
	Driver apply.Driver
	// Trigram carries the once-per-target missing-pg_trgm advisory.
	Trigram *TrigramAdvisor
	// FailDump is where a failed snapshot script is written (empty: nowhere).
	FailDump string
	// Verbose adds the per-delta write shape and timing to the apply log.
	// The batch-level shape line is printed either way.
	Verbose bool
}

// summaryTables caps how many tables a non-verbose batch summary names; the
// busiest win the line and the rest collapse into a count. Verbose lists all.
const summaryTables = 6

func (o Options) maxSummaryTables() int {
	if o.Verbose {
		return 0
	}
	return summaryTables
}

// PermanentError stops the reconnect loop (revoked credential, apply failure).
type PermanentError struct{ Inner error }

func (e PermanentError) Error() string { return "permanent: " + e.Inner.Error() }
func (e PermanentError) Unwrap() error { return e.Inner }

// IsPermanent reports whether the reconnect loop must stop.
func IsPermanent(err error) bool {
	var p PermanentError
	return errors.As(err, &p)
}

// Conn wraps the WebSocket with mutex-free sequential send (single goroutine).
type Conn struct {
	ws *websocket.Conn
}

// Dial opens the authenticated data-plane connection and sends the hello.
func Dial(ctx context.Context, wsURL, accessToken string) (*Conn, error) {
	header := http.Header{}
	header.Set("Authorization", "Bearer "+accessToken)
	header.Set("User-Agent", Agent)
	ws, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	ws.SetReadLimit(maxReadBytes)
	c := &Conn{ws: ws}
	if err := c.Send(ctx, framing.Hello{Type: "hello", Version: framing.ProtocolVersion, Agent: Agent}); err != nil {
		_ = ws.Close(websocket.StatusInternalError, "hello failed")
		return nil, err
	}
	return c, nil
}

// DialHost opens the authenticated control-plane connection (managed mode)
// and sends the host hello carrying the base DSN's dialect.
func DialHost(ctx context.Context, wsURL, accessToken, dialect string) (*Conn, error) {
	header := http.Header{}
	header.Set("Authorization", "Bearer "+accessToken)
	header.Set("User-Agent", Agent)
	ws, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		return nil, fmt.Errorf("dial host: %w", err)
	}
	ws.SetReadLimit(maxReadBytes)
	c := &Conn{ws: ws}
	hello := framing.HostHello{
		Type: "hello", Version: framing.ProtocolVersion, Agent: Agent, Dialect: dialect,
	}
	if err := c.Send(ctx, hello); err != nil {
		_ = ws.Close(websocket.StatusInternalError, "hello failed")
		return nil, err
	}
	return c, nil
}

// RecvHost reads one control-plane frame (host protocol decoder).
func (c *Conn) RecvHost(ctx context.Context) (any, error) {
	_, raw, err := c.ws.Read(ctx)
	if err != nil {
		return nil, err
	}
	return framing.DecodeHost(raw)
}

func (c *Conn) Send(ctx context.Context, frame any) error {
	raw, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	return c.ws.Write(ctx, websocket.MessageText, raw)
}

func (c *Conn) Recv(ctx context.Context) (any, error) {
	_, raw, err := c.ws.Read(ctx)
	if err != nil {
		return nil, err
	}
	return framing.Decode(raw)
}

func (c *Conn) Close(code websocket.StatusCode, reason string) {
	_ = c.ws.Close(code, reason)
}

// Pump receives batches and applies them until the connection drops or an
// apply error halts the client. Returns nil only on context cancellation.
// rebootstrap re-applies the snapshot when the server pauses the feed with
// snapshot_required (publish model: rows written while a schema was unlinked
// exist only in the snapshot); nil logs and waits instead.
//
// bootstrapPending defers a first bootstrap that answered publish_required:
// the client idles connected, and the first push (the publish queues the
// bootstrap DDL delta) triggers the snapshot before any delta is applied —
// replay-safe either way, since the snapshot and the queued deltas carry the
// same revision-guarded statements.
func Pump(ctx context.Context, conn *Conn, db *sql.DB, logger *log.Logger,
	rebootstrap func(context.Context) error, bootstrapPending bool,
	opts Options) error {
	// Keepalive: drives intermediate proxies; the server answers pong frames.
	pingCtx, pingCancel := context.WithCancel(ctx)
	defer pingCancel()
	go func() {
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-pingCtx.Done():
				return
			case <-ticker.C:
				_ = conn.Send(pingCtx, framing.Ping{Type: "ping"})
			}
		}
	}()

	for {
		frame, err := conn.Recv(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		switch f := frame.(type) {
		case framing.Batch:
			if f.SnapshotRequired || bootstrapPending {
				if rebootstrap == nil {
					logger.Printf("server paused the feed: re-run 'ee-database run' to re-apply the snapshot")
					continue
				}
				if bootstrapPending {
					logger.Printf("first publish landed: bootstrapping from the snapshot…")
				} else {
					logger.Printf("server paused the feed: re-applying the snapshot…")
				}
				if err := rebootstrap(ctx); err != nil {
					if errors.Is(err, auth.ErrPublishRequired) {
						// A push raced the publish state — keep idling.
						logger.Printf("still waiting for the first schema publish…")
						continue
					}
					return fmt.Errorf("re-bootstrap: %w", err)
				}
				bootstrapPending = false
				if f.SnapshotRequired {
					continue
				}
			}
			if len(f.Deltas) == 0 {
				continue
			}
			// "in one transaction" is not decoration: the window IS the
			// replica's transaction unit (the server never ends a window
			// inside a batch_id), which is the grouping an operator sizing a
			// re-enrichment has to know.
			logger.Printf("applying %d delta(s) (%d .. %d) in one transaction",
				len(f.Deltas), f.Deltas[0].ID, f.Deltas[len(f.Deltas)-1].ID)
			for i := range f.Deltas {
				opts.Trigram.Check(f.Deltas[i].SQL, logger)
			}
			res, err := apply.Batch(ctx, db, f.Deltas, opts.Verbose)
			if err != nil {
				msg := err.Error()
				failedID := res.FailedID
				if failedID != nil {
					for i := range f.Deltas {
						if f.Deltas[i].ID == *failedID {
							msg = opts.Driver.DescribeError(f.Deltas[i].SQL, err)
							break
						}
					}
				}
				logger.Printf("delta apply failed:\n%s", msg)
				if err := conn.Send(ctx, framing.ApplyError{
					Type: "apply_error", DeltaID: failedID, Message: msg,
				}); err != nil {
					return err
				}
				if failedID == nil {
					// Nothing to quarantine server-side (the failure did not
					// name a delta), so the same window would come back
					// unchanged — stop instead of spinning.
					time.Sleep(300 * time.Millisecond)
					conn.Close(websocket.StatusNormalClosure, "apply failed")
					return PermanentError{Inner: fmt.Errorf(
						"SQL apply failed — fix the cause and rerun 'ee-database run': %w", err)}
				}
				// The server quarantines that enrichment's whole batch and
				// re-pushes without it, so the next frame carries the
				// FOLLOWING enrichments: stay connected and keep applying
				// instead of exiting and stalling the queue behind one bad
				// row. The quarantined work stays visible in the Database
				// Sync page's Quarantine tab until it is reinjected or dropped.
				logger.Printf(
					"delta %d quarantined server-side; continuing with the rest of the queue",
					*failedID,
				)
				continue
			}
			for _, d := range res.Deltas {
				logger.Printf("  delta %d %s %s%s (%s) — %s", d.ID, d.Kind, d.Op,
					entitySuffix(d.EntityType), roundedDuration(d.Elapsed),
					d.Shape.Summary(0))
			}
			// The write shape, said once per window: an acked delta is gone
			// server-side, so this log line is the only lasting record of what
			// one enrichment costs this replica (issue #139).
			logger.Printf("applied %d delta(s) in %s — %s", len(f.Deltas),
				roundedDuration(res.Elapsed), res.Shape.Summary(opts.maxSummaryTables()))
			if err := conn.Send(ctx, framing.Ack{Type: "ack", UpToID: res.LastID}); err != nil {
				return err
			}
			logger.Printf("acked up to delta %d", res.LastID)
		case framing.Pong:
			// Heartbeat reply; nothing to do.
		default:
			logger.Printf("unexpected frame: %T", frame)
		}
	}
}

// Bootstrap fetches and applies the snapshot over the credential. On apply
// failure the database's full complaint is logged, the fetched script is
// saved to opts.FailDump (when non-empty) so the failing statement can be
// inspected, and the complaint is reported to the server, which shows it on
// the Sync client card; a later success removes the stale dump. Under
// opts.Verbose the applied snapshot's own write shape is reported.
func Bootstrap(ctx context.Context, client *auth.Client, accessToken string, db *sql.DB,
	logger *log.Logger, opts Options) error {
	logger.Printf("bootstrapping: fetching snapshot…")
	sqlText, cursor, err := client.FetchSnapshot(ctx, accessToken)
	if err != nil {
		return err
	}
	opts.Trigram.Check(sqlText, logger)
	failDump := opts.FailDump
	started := time.Now()
	if err := opts.Driver.Snapshot(ctx, db, sqlText); err != nil {
		described := opts.Driver.DescribeError(sqlText, err)
		logger.Printf("snapshot apply failed (nothing applied — single transaction):\n%s", described)
		if failDump != "" {
			// The script only exists in this response — persist it (0600: it
			// carries the org's entity data) or the error is undiagnosable.
			if werr := os.WriteFile(failDump, []byte(sqlText), 0o600); werr != nil {
				logger.Printf("could not save the failed snapshot: %v", werr)
			} else {
				logger.Printf("failed snapshot saved to %s", failDump)
			}
		}
		// Without this the server knows nothing: a first bootstrap has no
		// WebSocket to carry an apply_error frame, and the page would keep
		// asking for a client that is in fact running. An interrupted run has
		// nothing to report.
		if ctx.Err() == nil {
			if rerr := client.ReportApplyError(ctx, accessToken, "snapshot: "+described); rerr != nil {
				logger.Printf("could not report the failed snapshot to the server: %v", rerr)
			}
		}
		return fmt.Errorf("apply snapshot: %w", err)
	}
	if failDump != "" {
		_ = os.Remove(failDump) // evidence from an earlier failed attempt
	}
	if opts.Verbose {
		// The snapshot is the delta from zero: its shape is the table
		// inventory the replica starts from.
		shape := apply.Summarize(sqlText)
		logger.Printf("snapshot applied in %s — %s",
			roundedDuration(time.Since(started)), shape.Summary(0))
	}
	logger.Printf("bootstrap complete (delta cursor %d)", cursor)
	return nil
}

// entitySuffix renders the delta's entity type when the server named one.
func entitySuffix(entityType string) string {
	if entityType == "" {
		return ""
	}
	return " " + entityType
}

// roundedDuration keeps apply timings readable at every scale a delta spans:
// a one-statement DDL delta lands in microseconds and must not print as "0s".
func roundedDuration(d time.Duration) time.Duration {
	switch {
	case d < time.Millisecond:
		return d.Round(10 * time.Microsecond)
	case d < time.Second:
		return d.Round(time.Millisecond)
	default:
		return d.Round(10 * time.Millisecond)
	}
}

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
	"time"

	"github.com/coder/websocket"

	"github.com/TOT-Concept/ee-database/internal/apply"
	"github.com/TOT-Concept/ee-database/internal/framing"
	"github.com/TOT-Concept/ee-database/internal/auth"
)

const (
	pingInterval = 30 * time.Second
	// Batches carry rendered SQL for up to page_limit deltas.
	maxReadBytes = 64 * 1024 * 1024
)

// Agent identifies this client in User-Agent and hello frames.
const Agent = "ee-database/0.1.0"

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
func Pump(ctx context.Context, conn *Conn, db *sql.DB, logger *log.Logger,
	rebootstrap func(context.Context) error) error {
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
			if f.SnapshotRequired {
				if rebootstrap == nil {
					logger.Printf("server paused the feed: re-run 'ee-database run' to re-apply the snapshot")
					continue
				}
				logger.Printf("server paused the feed: re-applying the snapshot…")
				if err := rebootstrap(ctx); err != nil {
					return fmt.Errorf("re-bootstrap: %w", err)
				}
				continue
			}
			if len(f.Deltas) == 0 {
				continue
			}
			logger.Printf("applying %d delta(s) (%d .. %d)",
				len(f.Deltas), f.Deltas[0].ID, f.Deltas[len(f.Deltas)-1].ID)
			lastID, failedID, err := apply.Batch(ctx, db, f.Deltas)
			if err != nil {
				msg := err.Error()
				_ = conn.Send(ctx, framing.ApplyError{
					Type: "apply_error", DeltaID: failedID, Message: msg,
				})
				// Give the frame a moment to flush before closing.
				time.Sleep(300 * time.Millisecond)
				conn.Close(websocket.StatusNormalClosure, "apply failed")
				return PermanentError{Inner: fmt.Errorf(
					"SQL apply failed — fix the cause and rerun 'ee-database run': %w", err)}
			}
			if err := conn.Send(ctx, framing.Ack{Type: "ack", UpToID: lastID}); err != nil {
				return err
			}
			logger.Printf("acked up to delta %d", lastID)
		case framing.Pong:
			// Heartbeat reply; nothing to do.
		default:
			logger.Printf("unexpected frame: %T", frame)
		}
	}
}

// Bootstrap fetches and applies the snapshot over the credential.
func Bootstrap(ctx context.Context, client *auth.Client, accessToken string, db *sql.DB, logger *log.Logger) error {
	logger.Printf("bootstrapping: fetching snapshot…")
	sqlText, cursor, err := client.FetchSnapshot(ctx, accessToken)
	if err != nil {
		return err
	}
	if err := apply.Snapshot(ctx, db, sqlText); err != nil {
		return fmt.Errorf("apply snapshot: %w", err)
	}
	logger.Printf("bootstrap complete (delta cursor %d)", cursor)
	return nil
}

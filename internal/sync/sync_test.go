package sync

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/TOT-Concept/ee-database/internal/apply"
	"github.com/TOT-Concept/ee-database/internal/auth"
)

// stubDriver stands in for a database type: it records the script Bootstrap
// hands it and answers with a canned error.
type stubDriver struct {
	apply.Driver
	applied []string
	fail    error
}

func (d *stubDriver) Snapshot(_ context.Context, _ *sql.DB, script string) error {
	d.applied = append(d.applied, script)
	return d.fail
}

func (d *stubDriver) DescribeError(script string, err error) string {
	return "described[" + err.Error() + "] over " + script
}

// syncServer serves the snapshot and receives what the client reports about
// applying it.
type syncServer struct {
	script       string
	reportStatus int

	mu      sync.Mutex
	reports []string // "<authorization> <message>" per apply-error call
}

func (s *syncServer) reported() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.reports
	s.reports = nil
	return out
}

func (s *syncServer) client(t *testing.T) *auth.Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/database-sync/snapshot":
			w.Header().Set("X-EE-Next-Cursor", "42")
			_, _ = io.WriteString(w, s.script)
		case "/api/database-sync/apply-error":
			var body struct {
				Message string `json:"message"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("apply-error body: %v", err)
			}
			s.mu.Lock()
			s.reports = append(s.reports, r.Header.Get("Authorization")+" "+body.Message)
			s.mu.Unlock()
			w.WriteHeader(s.reportStatus)
		default:
			t.Errorf("unexpected call to %s", r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)
	return auth.New(server.URL, Agent)
}

// The snapshot is applied by the target's driver, and what the driver says
// about a failure is what the operator reads — in the log, and on the Sync
// client card through the server.
func TestBootstrapAppliesThroughTheDriver(t *testing.T) {
	const script = "BEGIN; CREATE TABLE t (k text); COMMIT;"
	server := &syncServer{script: script, reportStatus: http.StatusNoContent}
	client := server.client(t)
	dump := filepath.Join(t.TempDir(), "snapshot-failed.sql")
	var logged bytes.Buffer
	logger := log.New(&logged, "", 0)

	driver := &stubDriver{fail: errors.New("boom")}
	opts := Options{Driver: driver, FailDump: dump}
	err := Bootstrap(context.Background(), client, "access", nil, logger, opts)
	if err == nil || err.Error() != "apply snapshot: boom" || IsPermanent(err) {
		t.Fatalf("err = %v, want a retryable apply failure", err)
	}
	if len(driver.applied) != 1 || driver.applied[0] != script {
		t.Fatalf("driver received %q", driver.applied)
	}
	described := "described[boom] over " + script
	if !strings.Contains(logged.String(), described) {
		t.Errorf("log = %q, want %q", logged.String(), described)
	}
	if saved, err := os.ReadFile(dump); err != nil || string(saved) != script {
		t.Errorf("failed snapshot not kept: %q, %v", saved, err)
	}
	if got := server.reported(); len(got) != 1 || got[0] != "Bearer access snapshot: "+described {
		t.Errorf("reported to the server: %q", got)
	}

	// The next success clears the evidence of the failed attempt, and has
	// nothing to report.
	driver.fail = nil
	logged.Reset()
	if err := Bootstrap(context.Background(), client, "access", nil, logger, opts); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dump); !os.IsNotExist(err) {
		t.Errorf("stale dump still there (err = %v)", err)
	}
	if !strings.Contains(logged.String(), "bootstrap complete (delta cursor 42)") {
		t.Errorf("log = %q", logged.String())
	}
	if got := server.reported(); len(got) != 0 {
		t.Errorf("a successful bootstrap reported %q", got)
	}
}

// Reporting is best effort: a server that refuses the report changes nothing
// about the failure the caller retries on.
func TestBootstrapSurvivesAFailedReport(t *testing.T) {
	server := &syncServer{script: "SELECT 1;", reportStatus: http.StatusInternalServerError}
	var logged bytes.Buffer
	driver := &stubDriver{fail: errors.New("boom")}
	err := Bootstrap(context.Background(), server.client(t), "access", nil,
		log.New(&logged, "", 0), Options{Driver: driver})
	if err == nil || err.Error() != "apply snapshot: boom" || IsPermanent(err) {
		t.Fatalf("err = %v, want the apply failure", err)
	}
	if want := "could not report the failed snapshot to the server: apply-error: server returned 500"; !strings.Contains(logged.String(), want) {
		t.Errorf("log = %q, want %q", logged.String(), want)
	}
}

// A run interrupted while the snapshot applies fails with the context's
// error: there is no database complaint to put on the Sync client card.
func TestBootstrapDoesNotReportAnInterruptedRun(t *testing.T) {
	server := &syncServer{script: "SELECT 1;", reportStatus: http.StatusNoContent}
	client := server.client(t)
	ctx, cancel := context.WithCancel(context.Background())
	driver := &cancellingDriver{stubDriver: stubDriver{fail: context.Canceled}, cancel: cancel}
	var logged bytes.Buffer
	err := Bootstrap(ctx, client, "access", nil, log.New(&logged, "", 0), Options{Driver: driver})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want the cancellation", err)
	}
	if got := server.reported(); len(got) != 0 {
		t.Errorf("an interrupted run reported %q", got)
	}
	if strings.Contains(logged.String(), "could not report") {
		t.Errorf("log = %q", logged.String())
	}
}

// cancellingDriver is interrupted while it applies the snapshot.
type cancellingDriver struct {
	stubDriver
	cancel context.CancelFunc
}

func (d *cancellingDriver) Snapshot(ctx context.Context, db *sql.DB, script string) error {
	d.cancel()
	return d.stubDriver.Snapshot(ctx, db, script)
}

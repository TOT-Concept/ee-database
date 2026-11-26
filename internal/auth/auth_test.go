package auth

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"
)

const testAgent = "ee-database/9.9.9"

// upgradeBody is the 426 the server answers (FastAPI wraps the detail).
const upgradeBody = `{"detail": {"code": "client_upgrade_required", "minimum": "1.7.0", ` +
	`"detail": "This database sync needs ee-database 1.7.0 or later — download it from the Database Sync page"}}`

const upgradeSentence = "This database sync needs ee-database 1.7.0 or later — " +
	"download it from the Database Sync page"

func serve(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return New(server.URL, testAgent)
}

// The server's minimum-version gate reads the User-Agent of the exchange and
// the claim; every call carries it so no route is ever read as "too old".
func TestEveryRequestIdentifiesTheClient(t *testing.T) {
	var mu sync.Mutex
	agents := map[string]string{}
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		agents[r.URL.Path] = r.Header.Get("User-Agent")
		mu.Unlock()
		if r.URL.Path == "/api/database-sync/apply-error" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_, _ = io.WriteString(w, "{}")
	})
	ctx := context.Background()
	calls := map[string]func() error{
		"exchange":      func() error { _, err := c.Exchange(ctx, "tok"); return err },
		"snapshot":      func() error { _, _, err := c.FetchSnapshot(ctx, "tok"); return err },
		"apply-error":   func() error { return c.ReportApplyError(ctx, "tok", "boom") },
		"device-code":   func() error { _, err := c.StartDeviceCode(ctx, ""); return err },
		"poll":          func() error { _, err := c.PollDeviceCode(ctx, "code"); return err },
		"host-exchange": func() error { _, err := c.HostExchange(ctx, "tok"); return err },
		"claim":         func() error { _, err := c.Claim(ctx, "tok", "db-1"); return err },
	}
	for name, call := range calls {
		if err := call(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	var seen []string
	for path, agent := range agents {
		seen = append(seen, strings.TrimPrefix(path, "/api/database-sync/"))
		if agent != testAgent {
			t.Errorf("%s was sent as %q, want %q", path, agent, testAgent)
		}
	}
	sort.Strings(seen)
	want := "apply-error claim device-code exchange host-exchange poll snapshot"
	if got := strings.Join(seen, " "); got != want {
		t.Errorf("routes called = %q, want %q", got, want)
	}
}

func TestUpgradeRequiredCarriesTheServersSentence(t *testing.T) {
	c := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUpgradeRequired)
		_, _ = io.WriteString(w, upgradeBody)
	})
	ctx := context.Background()
	_, exchangeErr := c.Exchange(ctx, "tok")
	_, claimErr := c.Claim(ctx, "tok", "db-1")
	for name, err := range map[string]error{"exchange": exchangeErr, "claim": claimErr} {
		var upgrade ErrUpgradeRequired
		if !errors.As(err, &upgrade) {
			t.Errorf("%s: err = %v, want ErrUpgradeRequired", name, err)
			continue
		}
		if err.Error() != upgradeSentence {
			t.Errorf("%s: message = %q, want the server's sentence alone", name, err)
		}
	}
}

// A 426 that is not the application's (a proxy's) still stops the client with
// whatever the body said.
func TestUpgradeRequiredWithAForeignBody(t *testing.T) {
	c := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUpgradeRequired)
		_, _ = io.WriteString(w, "Upgrade Required\n")
	})
	_, err := c.Exchange(context.Background(), "tok")
	var upgrade ErrUpgradeRequired
	if !errors.As(err, &upgrade) ||
		err.Error() != "the server requires a newer ee-database: Upgrade Required" {
		t.Errorf("err = %v", err)
	}
}

func TestOtherRefusalsAreNotUpgrades(t *testing.T) {
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/claim") {
			w.WriteHeader(http.StatusConflict)
		} else {
			w.WriteHeader(http.StatusUnauthorized)
		}
		_, _ = io.WriteString(w, `{"detail": "nope"}`)
	})
	ctx := context.Background()
	var upgrade ErrUpgradeRequired
	_, err := c.Exchange(ctx, "tok")
	if errors.As(err, &upgrade) || !strings.Contains(err.Error(), "401") {
		t.Errorf("exchange: err = %v, want the plain 401", err)
	}
	_, err = c.Claim(ctx, "tok", "db-1")
	var notClaimable ErrNotClaimable
	if !errors.As(err, &notClaimable) {
		t.Errorf("claim: err = %v, want ErrNotClaimable", err)
	}
}

func TestReportApplyError(t *testing.T) {
	type request struct {
		method, path, auth, contentType string
		body                            map[string]string
	}
	var mu sync.Mutex
	var last request
	status := http.StatusNoContent
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		last = request{
			method: r.Method, path: r.URL.Path,
			auth: r.Header.Get("Authorization"), contentType: r.Header.Get("Content-Type"),
		}
		if err := json.NewDecoder(r.Body).Decode(&last.body); err != nil {
			t.Errorf("body is not JSON: %v", err)
		}
		w.WriteHeader(status)
		if status != http.StatusNoContent {
			_, _ = io.WriteString(w, `{"detail": "credential revoked"}`)
		}
	})
	report := func(message string) (request, error) {
		err := c.ReportApplyError(context.Background(), "access-1", message)
		mu.Lock()
		defer mu.Unlock()
		return last, err
	}

	message := "snapshot: ERROR: type \"nosuchtype\" does not exist (SQLSTATE 42704)\n  script line 3: é"
	got, err := report(message)
	if err != nil {
		t.Fatal(err)
	}
	if got.method != http.MethodPost || got.path != "/api/database-sync/apply-error" ||
		got.auth != "Bearer access-1" || got.contentType != "application/json" {
		t.Errorf("request = %+v", got)
	}
	if len(got.body) != 1 || got.body["message"] != message {
		t.Errorf("body = %q", got.body)
	}

	// The server refuses a message over its limit: the head is what names the
	// error, so the tail is cut — counted in characters, like the server does.
	got, err = report(strings.Repeat("é", maxApplyErrorChars+500))
	if err != nil {
		t.Fatal(err)
	}
	if n := utf8.RuneCountInString(got.body["message"]); n != maxApplyErrorChars {
		t.Errorf("long message sent with %d characters, want %d", n, maxApplyErrorChars)
	}

	mu.Lock()
	status = http.StatusUnauthorized
	mu.Unlock()
	_, err = report(message)
	if err == nil || !strings.Contains(err.Error(), "401") || !strings.Contains(err.Error(), "credential revoked") {
		t.Errorf("err = %v, want the server's refusal", err)
	}
}

// The entity layer holds values the target type cannot store: no snapshot
// exists, and the operator reads which values, one per line.
func TestSnapshotRefusedForTargetLimits(t *testing.T) {
	body := `{"detail": {"code": "values_exceed_target_limits", "dialect": "sqlserver", "total": 3, "items": [` +
		`{"entity_type": "Film", "table": "film", "column": "title", "reason": "longer than 400 characters, ` +
		`the limit of a key or indexed text column on SQL Server"}, ` +
		`{"entity_type": "Star", "table": "star", "column": "mass", "reason": "outside what DECIMAL(38,10) holds; ` +
		`declare db_type: double for this property"}]}}`
	c := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, body)
	})
	_, _, err := c.FetchSnapshot(context.Background(), "access-1")
	// The sentence is what a retry line repeats; the values are told apart.
	const sentence = "snapshot: the target database cannot hold 3 value(s) of the entity layer — " +
		"shorten them or change the schema; the sync resumes by itself"
	if err == nil || err.Error() != sentence {
		t.Errorf("err = %v\nwant %s", err, sentence)
	}
	const items = "  Film: film.title longer than 400 characters, the limit of a key or indexed text column on SQL Server\n" +
		"  Star: star.mass outside what DECIMAL(38,10) holds; declare db_type: double for this property\n" +
		"  … and 1 more"
	var limits ErrTargetLimits
	if !errors.As(err, &limits) || limits.Total != 3 || limits.Items != items {
		t.Errorf("limits = %+v\nwant the items:\n%s", limits, items)
	}
	// It is not the wait for a first publish, which idles instead of retrying.
	if errors.Is(err, ErrPublishRequired) {
		t.Error("a target-limits refusal must not read as publish_required")
	}
}

func TestSnapshotOtherConflicts(t *testing.T) {
	var mu sync.Mutex
	body := `{"detail": {"code": "publish_required"}}`
	c := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, body)
	})
	if _, _, err := c.FetchSnapshot(context.Background(), "tok"); !errors.Is(err, ErrPublishRequired) {
		t.Errorf("err = %v, want ErrPublishRequired", err)
	}
	mu.Lock()
	body = `{"detail": {"code": "target_changed"}}`
	mu.Unlock()
	_, _, err := c.FetchSnapshot(context.Background(), "tok")
	if err == nil || err.Error() != `snapshot: server returned 409: {"detail": {"code": "target_changed"}}` {
		t.Errorf("err = %v, want the server's answer as it is", err)
	}
}

// Pairing (token paste: exchange; browser: poll) and a host's claim all learn
// which generation of the target database the credential belongs to.
func TestResponsesCarryTheTargetGeneration(t *testing.T) {
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		switch strings.TrimPrefix(r.URL.Path, "/api/database-sync/") {
		case "exchange":
			_, _ = io.WriteString(w, `{"access_token": "a", "expires_in": 900, "database_id": "db-1", `+
				`"database_name": "Films", "dialect": "sqlserver", "target_generation": 3}`)
		case "poll":
			_, _ = io.WriteString(w, `{"status": "ok", "refresh_token": "r", "database_id": "db-1", `+
				`"database_name": "Films", "dialect": "sqlserver", "target_generation": 3}`)
		case "claim":
			_, _ = io.WriteString(w, `{"refresh_token": "r", "server_url": "x", "database_id": "db-1", `+
				`"database_name": "Films", "dialect": "sqlserver", "suggested_database_name": "films__3", `+
				`"target_generation": 3}`)
		}
	})
	ctx := context.Background()
	exchange, err := c.Exchange(ctx, "tok")
	if err != nil || exchange.TargetGeneration != 3 || exchange.Dialect != "sqlserver" {
		t.Errorf("exchange = %+v, err = %v", exchange, err)
	}
	poll, err := c.PollDeviceCode(ctx, "code")
	if err != nil || poll.TargetGeneration != 3 || poll.Dialect != "sqlserver" {
		t.Errorf("poll = %+v, err = %v", poll, err)
	}
	claim, err := c.Claim(ctx, "tok", "db-1")
	if err != nil || claim.TargetGeneration != 3 || claim.SuggestedDatabaseName != "films__3" {
		t.Errorf("claim = %+v, err = %v", claim, err)
	}
}

package cli

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/TOT-Concept/ee-database/internal/apply"
	"github.com/TOT-Concept/ee-database/internal/auth"
	"github.com/TOT-Concept/ee-database/internal/config"
	"github.com/TOT-Concept/ee-database/internal/framing"
)

// What happens to a pairing when the server points the registration at
// another database ("Change target database"): the count of those changes —
// the target generation — travels with every pairing and claim.

func exchangeAt(dialect string, generation *atomic.Int64) func() (int, string) {
	return func() (int, string) {
		return http.StatusOK, fmt.Sprintf(`{"access_token": "access", "expires_in": 900, `+
			`"database_id": "db-1", "database_name": "Films", "dialect": %q, "target_generation": %d}`,
			dialect, generation.Load())
	}
}

func profileState(t *testing.T) (cfg *config.Config, token, dsn string) {
	t.Helper()
	cfg, token, err := config.LoadProfile("db-1")
	if err != nil {
		t.Fatalf("profile db-1: %v", err)
	}
	return cfg, token, config.LoadProfileDSN("db-1")
}

func TestRepairKeepsTheDSNUntilTheTargetChanges(t *testing.T) {
	isolateConfig(t)
	var generation atomic.Int64
	client := newAuthClient(fakeServer(t, map[string]func() (int, string){
		"exchange": exchangeAt("sqlserver", &generation),
	}))
	pair := func(token string) string {
		t.Helper()
		var stdout, stderr bytes.Buffer
		if code := runManualPair(client, token, &stdout, &stderr); code != 0 {
			t.Fatalf("pair exit %d: %s", code, stderr.String())
		}
		return stdout.String()
	}
	const changed = "The target database of"
	const savedDSN = "sqlserver://u:p@db:1433?database=films"

	// A profile made by a release that did not count generations.
	if err := config.SaveProfile("db-1", &config.Config{
		DatabaseID: "db-1", DatabaseName: "Films", Dialect: "sqlserver", Bootstrapped: true,
	}, "token-0"); err != nil {
		t.Fatal(err)
	}
	if err := config.SaveProfileDSN("db-1", savedDSN); err != nil {
		t.Fatal(err)
	}

	// Pairing again rotates the credential: same target, the DSN stays.
	out := pair("token-1")
	cfg, token, dsn := profileState(t)
	if strings.Contains(out, changed) || dsn != savedDSN || token != "token-1" {
		t.Errorf("rotation: out %q, token %q, dsn %q", out, token, dsn)
	}
	if cfg.TargetGeneration != 0 || cfg.Bootstrapped {
		t.Errorf("rotation: cfg = %+v — a new pairing bootstraps again", cfg)
	}

	// The target was changed on the server: the saved DSN names the old one.
	generation.Store(1)
	out = pair("token-2")
	cfg, token, dsn = profileState(t)
	want := `  The target database of "Films" changed (now SQL Server). ` +
		"The saved DSN was discarded: pass --dsn on the next run.\n"
	if !strings.Contains(out, want) {
		t.Errorf("retarget: out = %q, want %q in it", out, want)
	}
	if dsn != "" || token != "token-2" || cfg.TargetGeneration != 1 {
		t.Errorf("retarget: token %q, dsn %q, cfg %+v", token, dsn, cfg)
	}

	// Same generation again: nothing to say, and a DSN saved since stays.
	if err := config.SaveProfileDSN("db-1", savedDSN); err != nil {
		t.Fatal(err)
	}
	if out = pair("token-3"); strings.Contains(out, changed) || config.LoadProfileDSN("db-1") != savedDSN {
		t.Errorf("second rotation: out %q, dsn %q", out, config.LoadProfileDSN("db-1"))
	}

	// Changed again while no DSN is saved (it is passed on each run): the
	// operator is still told, without a DSN being said discarded.
	if err := config.ClearProfileDSN("db-1"); err != nil {
		t.Fatal(err)
	}
	generation.Store(2)
	out = pair("token-4")
	want = `  The target database of "Films" changed (now SQL Server). ` +
		"Pass the new database's --dsn on the next run.\n"
	if !strings.Contains(out, want) {
		t.Errorf("retarget without a saved dsn: out = %q, want %q in it", out, want)
	}
	if cfg, _, _ = profileState(t); cfg.TargetGeneration != 2 {
		t.Errorf("cfg = %+v", cfg)
	}
}

func TestFirstPairingRecordsTheGeneration(t *testing.T) {
	isolateConfig(t)
	var generation atomic.Int64
	generation.Store(4)
	client := newAuthClient(fakeServer(t, map[string]func() (int, string){
		"exchange": exchangeAt("postgres", &generation),
	}))
	var stdout, stderr bytes.Buffer
	if code := runManualPair(client, "token-1", &stdout, &stderr); code != 0 {
		t.Fatalf("pair exit %d: %s", code, stderr.String())
	}
	if strings.Contains(stdout.String(), "changed") {
		t.Errorf("a first pairing has no previous target: %q", stdout.String())
	}
	if cfg, _, _ := profileState(t); cfg.TargetGeneration != 4 || cfg.Dialect != "postgres" {
		t.Errorf("cfg = %+v", cfg)
	}
}

// The exchange completes a profile paired before the server reported the
// database; it does not second-guess a database type the profile already has.
func TestHealProfileFillsOnlyWhatIsMissing(t *testing.T) {
	isolateConfig(t)
	access := &auth.ExchangeResponse{DatabaseID: "db-1", DatabaseName: "Films", Dialect: "postgres"}
	var logged bytes.Buffer
	logger := log.New(&logged, "", 0)

	legacy := &config.Config{ServerURL: "https://ee.example"}
	if err := config.SaveProfile(config.LegacyKey, legacy, "tok"); err != nil {
		t.Fatal(err)
	}
	healed := &target{key: config.LegacyKey, cfg: legacy}
	healProfile(healed, access, logger)
	cfg, _, _ := profileState(t)
	if healed.key != "db-1" || cfg.DatabaseID != "db-1" || cfg.DatabaseName != "Films" || cfg.Dialect != "postgres" {
		t.Errorf("healed key %q, cfg %+v", healed.key, cfg)
	}

	known := &target{key: "db-1", cfg: &config.Config{
		DatabaseID: "db-1", DatabaseName: "Films", Dialect: "sqlserver", ServerURL: "https://ee.example",
	}}
	healProfile(known, access, logger)
	if known.cfg.Dialect != "sqlserver" {
		t.Errorf("dialect rewritten to %q", known.cfg.Dialect)
	}
	if logged.Len() != 0 {
		t.Errorf("log = %q, want nothing", logged.String())
	}
}

// --- host pair --------------------------------------------------------------

func TestHostPairRefusesASecondDatabaseTypePerServer(t *testing.T) {
	isolateConfig(t)
	server := fakeServer(t, nil) // refused before the server is asked anything
	if err := config.SaveHost(&config.HostConfig{
		HostID: "host-1", HostName: "laptop", ServerURL: server, Dialect: "postgres",
	}, "eeh_old", unreachableBaseDSN, ""); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := cmdHostPair([]string{
		"--server", server + "/", "--dsn", "sqlserver://sa:pw@127.0.0.1:1", "eeh_new",
	}, &stdout, &stderr)
	want := "host pair: this machine is already paired with " + server + ` as a PostgreSQL host ("laptop"). ` +
		"One pairing serves one database type per server; run " +
		"'ee-database host disconnect --server " + server + "' first.\n"
	if code != 1 || stderr.String() != want {
		t.Errorf("exit %d, stderr %q, want %q", code, stderr.String(), want)
	}
	pairing, err := config.LoadHost(server)
	if err != nil || pairing.Refresh != "eeh_old" || pairing.Config.Dialect != "postgres" {
		t.Errorf("the existing pairing must be left alone: %+v, %v", pairing, err)
	}
}

func TestHostPairReplacesAPairingOfTheSameType(t *testing.T) {
	isolateConfig(t)
	server := fakeServer(t, map[string]func() (int, string){
		"host-exchange": func() (int, string) {
			return http.StatusOK, `{"access_token": "access", "host_id": "host-2", "host_name": "laptop-2"}`
		},
	})
	// "" is a pairing from before hosts recorded their type: PostgreSQL. A
	// type this release no longer knows served no database: replaceable too.
	for _, previous := range []string{"postgres", "", "mysql"} {
		if err := config.SaveHost(&config.HostConfig{
			HostID: "host-1", HostName: "laptop", ServerURL: server, Dialect: previous,
		}, "eeh_old", unreachableBaseDSN, ""); err != nil {
			t.Fatal(err)
		}
		var stdout, stderr bytes.Buffer
		// The rights check cannot reach the database: unknown, so not blocking.
		code := cmdHostPair([]string{"--server", server, "--dsn", unreachableBaseDSN, "eeh_new"}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("previous %q: exit %d, stderr %q", previous, code, stderr.String())
		}
		if want := "Replacing this machine's existing pairing with " + server + ` (host "laptop")`; !strings.Contains(stdout.String(), want) {
			t.Errorf("previous %q: stdout = %q, want %q in it", previous, stdout.String(), want)
		}
		pairing, err := config.LoadHost(server)
		if err != nil || pairing.Refresh != "eeh_new" || pairing.Config.HostID != "host-2" ||
			pairing.Config.Dialect != "postgres" {
			t.Errorf("previous %q: pairing = %+v, %v", previous, pairing, err)
		}
	}
}

// --- host run ---------------------------------------------------------------

func claimAt(name string, generation *atomic.Int64, claims *atomic.Int64) func() (int, string) {
	return func() (int, string) {
		n := claims.Add(1)
		g := generation.Load()
		suggested := name
		if g > 0 {
			suggested = fmt.Sprintf("%s__%d", name, g)
		}
		return http.StatusOK, fmt.Sprintf(`{"refresh_token": "claim-%d", "server_url": "x", `+
			`"database_id": "db-1", "database_name": "Films", "dialect": "postgres", `+
			`"suggested_database_name": %q, "target_generation": %d}`, n, suggested, g)
	}
}

// A profile claimed for the previous target is discarded whole — credential,
// DSN and bootstrap flag — and the registration is claimed afresh: the new
// target is a database of its own, named by the server.
func TestHostEnsureStartsOverWhenTheTargetChanged(t *testing.T) {
	var generation, claims atomic.Int64
	generation.Store(1)
	runner, logged := hostRunnerFor(t, fakeServer(t, map[string]func() (int, string){
		"host-exchange": hostExchangeOK,
		"claim":         claimAt("films", &generation, &claims),
	}))
	if err := config.SaveProfile("db-1", &config.Config{
		DatabaseID: "db-1", DatabaseName: "Films", Dialect: "postgres",
		ServerURL: runner.cfg.ServerURL, Bootstrapped: true,
	}, "old-token"); err != nil {
		t.Fatal(err)
	}
	if err := config.SaveProfileDSN("db-1", unreachableBaseDSN+"films"); err != nil {
		t.Fatal(err)
	}
	// The sync of the previous target is still running.
	stopped := make(chan struct{})
	runner.children["db-1"] = &managedChild{cancel: func() { close(stopped) }, done: stopped}

	runner.ensure(framing.HostRegistration{
		DatabaseID: "db-1", Name: "Films", Dialect: "postgres", TargetGeneration: 1, Claimable: true,
	})

	select {
	case <-stopped:
	default:
		t.Error("the running sync of the previous target was not stopped")
	}
	cfg, token, dsn := profileState(t)
	if cfg.TargetGeneration != 1 || cfg.Bootstrapped || token != "claim-1" {
		t.Errorf("cfg = %+v, token = %q", cfg, token)
	}
	if want := unreachableBaseDSN + "films__1"; dsn != want {
		t.Errorf("dsn = %q, want %q", dsn, want)
	}
	for _, want := range []string{
		"Films: the target database changed — claiming it afresh",
		"stopping db-1: target database changed",
		`claimed Films → local database "films__1"`,
		"Films: create-missing:", // the database server of this test does not exist
	} {
		if !strings.Contains(logged.String(), want) {
			t.Errorf("log = %q, want %q in it", logged.String(), want)
		}
	}

	// The same generation again is the profile's own target: no new claim.
	logged.Reset()
	runner.ensure(framing.HostRegistration{
		DatabaseID: "db-1", Name: "Films", Dialect: "postgres", TargetGeneration: 1, Claimable: true,
	})
	if claims.Load() != 1 || strings.Contains(logged.String(), "changed") {
		t.Errorf("claims = %d, log = %q", claims.Load(), logged.String())
	}
	if _, token, _ := profileState(t); token != "claim-1" {
		t.Errorf("token = %q", token)
	}
}

func revoked() (int, string) { return http.StatusUnauthorized, `{"detail": "Credential revoked"}` }

// managedTarget is a claimed profile on disk and its running form.
func managedTarget(t *testing.T, runner *hostRunner, generation int) *target {
	t.Helper()
	cfg := &config.Config{
		DatabaseID: "db-1", DatabaseName: "Films", Dialect: "postgres",
		ServerURL: runner.cfg.ServerURL, Bootstrapped: true, TargetGeneration: generation,
	}
	if err := config.SaveProfile("db-1", cfg, "old-token"); err != nil {
		t.Fatal(err)
	}
	if err := config.SaveProfileDSN("db-1", unreachableBaseDSN+"films"); err != nil {
		t.Fatal(err)
	}
	return &target{key: "db-1", cfg: cfg, refresh: "old-token", driver: runner.driver}
}

// A revoked credential is re-claimed by the host. When the claim says the
// target is still the same one, that is a rotation: the sync resumes on the
// same database.
func TestManagedSyncResumesAfterARotation(t *testing.T) {
	var generation, claims atomic.Int64
	runner, _ := hostRunnerFor(t, fakeServer(t, map[string]func() (int, string){
		"exchange":      revoked, // before and after the re-claim: one attempt per start
		"host-exchange": hostExchangeOK,
		"claim":         claimAt("films", &generation, &claims),
	}))
	target := managedTarget(t, runner, 0)
	var logged bytes.Buffer
	if _, retargeted := runner.syncManaged(context.Background(), target, nil, log.New(&logged, "", 0)); retargeted {
		t.Fatal("a rotation was taken for a change of target")
	}
	cfg, token, dsn := profileState(t)
	if token != "claim-1" || target.refresh != "claim-1" || dsn != unreachableBaseDSN+"films" || !cfg.Bootstrapped {
		t.Errorf("token %q, running token %q, dsn %q, cfg %+v", token, target.refresh, dsn, cfg)
	}
	for _, want := range []string{"re-claimed — resuming sync", "credential revoked again after re-claim"} {
		if !strings.Contains(logged.String(), want) {
			t.Errorf("log = %q, want %q in it", logged.String(), want)
		}
	}
}

// When the claim says the target changed, the open connection and the saved
// DSN are the previous target's: the sync ends instead of resuming there.
func TestManagedSyncStopsWhenTheReclaimNamesAnotherTarget(t *testing.T) {
	var generation, claims atomic.Int64
	generation.Store(1)
	runner, _ := hostRunnerFor(t, fakeServer(t, map[string]func() (int, string){
		"exchange":      revoked,
		"host-exchange": hostExchangeOK,
		"claim":         claimAt("films", &generation, &claims),
	}))
	target := managedTarget(t, runner, 0)
	var logged bytes.Buffer
	generationNow, retargeted := runner.syncManaged(context.Background(), target, nil, log.New(&logged, "", 0))
	if !retargeted || generationNow != 1 {
		t.Fatalf("retargeted = %v at generation %d — the change of target went unnoticed: %s",
			retargeted, generationNow, logged.String())
	}
	if want := "stopped: the target database changed — starting over on the new one\n"; !strings.Contains(logged.String(), want) {
		t.Errorf("log = %q, want %q in it", logged.String(), want)
	}
	if strings.Contains(logged.String(), "resuming") || target.refresh != "old-token" {
		t.Errorf("the sync resumed on the previous target: %q", logged.String())
	}
	// The stale profile is the control loop's to replace.
	if _, token, _ := profileState(t); token != "old-token" {
		t.Errorf("token = %q", token)
	}
}

// A sync that stopped on a change of target is started over by the control
// loop — unless the profile it ran on was replaced since: the server's notice
// of the change arrived after all, and that claim is the current one.
func TestRetargetedSyncIsStartedOverOnce(t *testing.T) {
	var generation, claims atomic.Int64
	generation.Store(1)
	runner, logged := hostRunnerFor(t, fakeServer(t, map[string]func() (int, string){
		"host-exchange": hostExchangeOK,
		"claim":         claimAt("films", &generation, &claims),
	}))
	managedTarget(t, runner, 0)
	found := retarget{from: 0, reg: framing.HostRegistration{
		DatabaseID: "db-1", Name: "Films", Dialect: "postgres", TargetGeneration: 1, Claimable: true,
	}}

	runner.retargets = []retarget{found}
	runner.restartRetargeted()
	cfg, token, dsn := profileState(t)
	if claims.Load() != 1 || cfg.TargetGeneration != 1 || cfg.Bootstrapped || token != "claim-1" ||
		dsn != unreachableBaseDSN+"films__1" {
		t.Fatalf("claims = %d, cfg = %+v, token %q, dsn %q\nlog: %s", claims.Load(), cfg, token, dsn, logged.String())
	}
	if len(runner.retargets) != 0 {
		t.Errorf("retargets left waiting: %+v", runner.retargets)
	}

	// The same finding again, as a second sync of the old profile would have
	// left it: the profile is the new target's now.
	runner.retargets = []retarget{found}
	runner.restartRetargeted()
	if _, token, _ := profileState(t); claims.Load() != 1 || token != "claim-1" {
		t.Errorf("claims = %d, token %q — the current claim was thrown away", claims.Load(), token)
	}
}

// The control loop restarts such a sync by itself, with the socket up and no
// frame from the server: nothing else would when the notice of the change was
// lost.
func TestHostControlLoopStartsARetargetedSyncOver(t *testing.T) {
	var generation, claims atomic.Int64
	generation.Store(1)
	claim := claimAt("films", &generation, &claims)
	answer := func(route func() (int, string)) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			status, body := route()
			w.WriteHeader(status)
			_, _ = io.WriteString(w, body)
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/database-sync/host-exchange", answer(hostExchangeOK))
	mux.HandleFunc("/api/database-sync/claim", answer(claim))
	mux.HandleFunc("/api/database-sync/host-ws", func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = ws.CloseNow() }()
		for { // hello, preflight — and never a frame back
			if _, _, err := ws.Read(r.Context()); err != nil {
				return
			}
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	runner, logged := hostRunnerFor(t, server.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner.ctx = ctx
	managedTarget(t, runner, 0)
	// What a child leaves behind when its re-claim names another target.
	runner.retargets = []retarget{{from: 0, reg: framing.HostRegistration{
		DatabaseID: "db-1", Name: "Films", Dialect: "postgres", TargetGeneration: 1, Claimable: true,
	}}}
	runner.wake <- struct{}{}

	stopped := make(chan error, 1)
	go func() {
		_, err := runner.connectAndPump()
		stopped <- err
	}()
	// The DSN is the last thing a claim writes.
	want := unreachableBaseDSN + "films__1"
	for deadline := time.Now().Add(20 * time.Second); config.LoadProfileDSN("db-1") != want; {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("the registration was not claimed afresh (control loop ended with %v)\nlog: %s", <-stopped, logged.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-stopped; !errors.Is(err, context.Canceled) {
		t.Errorf("control loop ended with %v", err)
	}
	if cfg, token, _ := profileState(t); cfg.TargetGeneration != 1 || token != "claim-1" || claims.Load() != 1 {
		t.Errorf("cfg = %+v, token %q, claims %d\nlog: %s", cfg, token, claims.Load(), logged.String())
	}
}

// The whole of it against a real server (skipped without its DSN): a host
// claims a registration and creates its database; the target is changed; the
// host lands in a fresh database — whether it hears of the change from the
// control plane, or from its own revoked credential first. The server of this
// test revokes every credential, so each sync loop ends by itself.
func TestHostFollowsAChangeOfTarget(t *testing.T) {
	for _, server := range liveServers {
		t.Run(server.dialect, func(t *testing.T) {
			baseDSN := os.Getenv(server.env)
			if baseDSN == "" {
				t.Skip(server.env + " is not set")
			}
			driver, err := apply.For(server.dialect)
			if err != nil {
				t.Fatal(err)
			}
			name := "ee_t222_go_" + randomHex(t)
			adminDSN, err := driver.DeriveDSN(baseDSN, server.maintenance)
			if err != nil {
				t.Fatal(err)
			}
			admin, err := driver.Open(adminDSN)
			if err != nil {
				t.Fatal(err)
			}
			exists := func(database string) bool {
				t.Helper()
				dsn, err := driver.DeriveDSN(baseDSN, database)
				if err != nil {
					t.Fatal(err)
				}
				db, err := driver.Open(dsn)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				return db.PingContext(ctx) == nil
			}
			t.Cleanup(func() {
				for _, database := range []string{name, name + "__1", name + "__2"} {
					if _, err := admin.ExecContext(context.Background(), server.drop(database)); err != nil {
						t.Errorf("cleanup of %s: %v", database, err)
					}
				}
				_ = admin.Close()
			})

			var generation, claims atomic.Int64
			claim := claimAt(name, &generation, &claims)
			runner, logged := hostRunnerFor(t, fakeServer(t, map[string]func() (int, string){
				"exchange":      revoked,
				"host-exchange": hostExchangeOK,
				"claim": func() (int, string) {
					status, body := claim()
					return status, strings.Replace(body, `"dialect": "postgres"`, `"dialect": "`+server.dialect+`"`, 1)
				},
			}))
			runner.driver, runner.baseDSN = driver, baseDSN
			runner.cfg.Dialect = server.dialect
			var stdout bytes.Buffer // written by ensure only, on this goroutine
			runner.stdout = &stdout
			assigned := func(frameGeneration int) {
				t.Helper()
				runner.ensure(framing.HostRegistration{
					DatabaseID: "db-1", Name: "Films", Dialect: server.dialect,
					TargetGeneration: frameGeneration, Claimable: true,
				})
				runner.wg.Wait() // the sync loop ends on its revoked credential
			}
			derived := func(database string) string {
				t.Helper()
				dsn, err := driver.DeriveDSN(baseDSN, database)
				if err != nil {
					t.Fatal(err)
				}
				return dsn
			}

			// First assignment: claimed, and its database created.
			assigned(0)
			cfg, _, dsn := profileState(t)
			if cfg.TargetGeneration != 0 || dsn != derived(name) || !exists(name) {
				t.Fatalf("first claim: cfg %+v, database exists %v\nlog: %s", cfg, exists(name), logged.String())
			}
			if want := "✓ Created database " + name + " for Films"; !strings.Contains(stdout.String(), want) {
				t.Errorf("stdout = %q, want %q in it", stdout.String(), want)
			}

			// The target changes, and the control plane says so.
			generation.Store(1)
			assigned(1)
			cfg, _, dsn = profileState(t)
			if cfg.TargetGeneration != 1 || dsn != derived(name+"__1") || !exists(name+"__1") {
				t.Fatalf("after the change: cfg %+v, dsn names %q: %v\nlog: %s",
					cfg, name+"__1", dsn == derived(name+"__1"), logged.String())
			}

			// It changes again, and the notice is lost: the host learns it
			// from the claim that follows its revoked credential. The sync
			// ends without touching the previous target, and hands the
			// registration to the control loop.
			generation.Store(2)
			assigned(1)
			if len(runner.children) != 0 || len(runner.retargets) != 1 || exists(name+"__2") {
				t.Fatalf("children = %d, handed over = %d, new database created early: %v\nlog: %s",
					len(runner.children), len(runner.retargets), exists(name+"__2"), logged.String())
			}
			select {
			case <-runner.wake:
			default:
				t.Fatal("the control loop was not told")
			}
			// The control loop starts it over, in a database of its own.
			runner.restartRetargeted()
			runner.wg.Wait()
			cfg, _, dsn = profileState(t)
			if cfg.TargetGeneration != 2 || dsn != derived(name+"__2") || !exists(name+"__2") {
				t.Fatalf("after the lost notice: cfg %+v\nlog: %s", cfg, logged.String())
			}
			// A later reconcile finds that profile current.
			assigned(2)
			if cfg, _, dsn = profileState(t); cfg.TargetGeneration != 2 || dsn != derived(name+"__2") {
				t.Errorf("after a later reconcile: cfg %+v\nlog: %s", cfg, logged.String())
			}
		})
	}
}

func randomHex(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 4)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(raw)
}

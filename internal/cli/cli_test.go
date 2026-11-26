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
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TOT-Concept/ee-database/internal/apply"
	"github.com/TOT-Concept/ee-database/internal/auth"
	"github.com/TOT-Concept/ee-database/internal/config"
	"github.com/TOT-Concept/ee-database/internal/framing"
	eesync "github.com/TOT-Concept/ee-database/internal/sync"
)

const upgradeSentence = "This database sync needs ee-database 9.0.0 or later — " +
	"download it from the Database Sync page"

// liveServers are the database servers of the live tests, each skipped without
// the variable that holds its superuser DSN.
var liveServers = []struct {
	dialect, env, maintenance string
	drop, dropLogin           func(name string) string
}{
	{"postgres", "EE_TEST_POSTGRES_DSN", "postgres", func(name string) string {
		return `DROP DATABASE IF EXISTS "` + name + `" WITH (FORCE)`
	}, func(name string) string {
		return `DROP ROLE IF EXISTS "` + name + `"`
	}},
	{"sqlserver", "EE_TEST_SQLSERVER_DSN", "master", func(name string) string {
		return "IF DB_ID(N'" + name + "') IS NOT NULL BEGIN ALTER DATABASE [" + name +
			"] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [" + name + "]; END"
	}, func(name string) string {
		return "IF SUSER_ID(N'" + name + "') IS NOT NULL DROP LOGIN [" + name + "]"
	}},
}

// isolateConfig points the profile store at a scratch directory.
func isolateConfig(t *testing.T) {
	t.Helper()
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(tmp, ".config"))
	t.Setenv("APPDATA", tmp)
}

// fakeServer answers the database-sync REST routes from a path → (status,
// body) table and fails the test on any other call.
func fakeServer(t *testing.T, routes map[string]func() (int, string)) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route, ok := routes[strings.TrimPrefix(r.URL.Path, "/api/database-sync/")]
		if !ok {
			t.Errorf("unexpected call to %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		status, body := route()
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(server.Close)
	return server.URL
}

func upgradeRequired() (int, string) {
	return http.StatusUpgradeRequired, `{"detail": {"code": "client_upgrade_required", ` +
		`"minimum": "9.0.0", "detail": "` + upgradeSentence + `"}}`
}

func TestPreflightSummary(t *testing.T) {
	yes, no := true, false
	cases := []struct {
		report         framing.PreflightReport
		databaseExists bool
		want           string
	}{
		{framing.PreflightReport{}, false, "create database unknown, DDL unknown, DML unknown"},
		{framing.PreflightReport{CreateDB: &yes, DDL: &yes, DML: &no}, false,
			"create database ok, DDL ok, DML MISSING"},
		// The trigram word is PostgreSQL's: it shows only when a report carries it.
		{framing.PreflightReport{CreateDB: &no, DDL: &yes, DML: &yes, Trigram: &no,
			Details: []string{"first", "second"}}, false,
			"create database MISSING, DDL ok, DML ok, pg_trgm MISSING — first; second"},
		// A host that may not create databases has a gap; a target whose
		// database is there has none, and the line does not read as a failure.
		{framing.PreflightReport{CreateDB: &no}, false, "create database MISSING, DDL unknown, DML unknown"},
		{framing.PreflightReport{CreateDB: &no, DDL: &yes, DML: &yes}, true,
			"DDL ok, DML ok, no create-database right (not needed: the target database exists)"},
		{framing.PreflightReport{CreateDB: &no, DDL: &yes, DML: &no, Trigram: &no,
			Details: []string{"first", "second"}}, true,
			"DDL ok, DML MISSING, pg_trgm MISSING, " +
				"no create-database right (not needed: the target database exists) — first; second"},
		{framing.PreflightReport{CreateDB: &yes, DDL: &yes, DML: &yes, Trigram: &yes}, true,
			"create database ok, DDL ok, DML ok, pg_trgm ok"},
		{framing.PreflightReport{DDL: &yes, DML: &yes}, true, "create database unknown, DDL ok, DML ok"},
	}
	for _, tc := range cases {
		if got := preflightSummary(tc.report, tc.databaseExists); got != tc.want {
			t.Errorf("summary = %q, want %q", got, tc.want)
		}
	}
}

func TestDescribeProfileNamesTheDialect(t *testing.T) {
	describe := func(dialect string) string {
		return describeProfile(config.Profile{Key: "db-1", Config: &config.Config{
			DatabaseName: "Films", Dialect: dialect, ServerURL: "https://ee.example",
		}})
	}
	// A profile paired before the server reported its dialect is PostgreSQL.
	for _, dialect := range []string{"", "postgres"} {
		if got := describe(dialect); got != "Films (postgres) @ https://ee.example" {
			t.Errorf("dialect %q described as %q", dialect, got)
		}
	}
	if got := describe("oracle"); got != "Films (oracle) @ https://ee.example" {
		t.Errorf("an unsupported dialect must still be shown, got %q", got)
	}
}

func TestNewTargetRefusesAnUnsupportedDialect(t *testing.T) {
	_, err := newTarget("db-1", &config.Config{Dialect: "oracle"}, "tok", "dsn", false)
	if err == nil || err.Error() != `unsupported dialect "oracle"` {
		t.Errorf("err = %v", err)
	}
	target, err := newTarget("db-1", &config.Config{}, "tok", "dsn", true)
	if err != nil || target.driver.Dialect() != "postgres" || !target.verbose {
		t.Errorf("target = %+v, err = %v", target, err)
	}
}

// The base DSN's scheme names the database type the host serves. Anything
// no driver reads is refused before the token is spent — fakeServer fails the
// test on any call — nothing is saved, and the password is never quoted.
func TestHostPairRefusesABaseDSNNoDriverReads(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
		want string
	}{
		{"an unknown database type", "oracle://u:secret@h:1521/",
			`host pair: --dsn: unsupported database type "oracle": postgres:// or sqlserver://` + "\n"},
		{"a connection string", "server=h;user id=sa;password=secret",
			"host pair: --dsn: not a URL: postgres://user:pass@host:5432/ or " +
				"sqlserver://user:pass@host:1433\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isolateConfig(t)
			server := fakeServer(t, nil)
			var stdout, stderr bytes.Buffer
			code := cmdHostPair([]string{"--server", server, "--dsn", tc.dsn, "token"}, &stdout, &stderr)
			if code != 2 || stderr.String() != tc.want || stdout.Len() != 0 {
				t.Errorf("exit %d, stdout %q\nstderr %q\nwant   %q", code, stdout.String(), stderr.String(), tc.want)
			}
			if _, err := config.LoadHost(server); !errors.Is(err, config.ErrHostNotPaired) {
				t.Errorf("a pairing was saved (err = %v)", err)
			}
		})
	}
	// What the URL parser accepts and the driver does not is refused too.
	for _, dsn := range []string{
		"postgres://u:secret@h:5432/?sslmode=sometimes",
		"sqlserver://sa:secret@h:1433?encrypt=sometimes",
	} {
		var stdout, stderr bytes.Buffer
		code := cmdHostPair([]string{"--server", fakeServer(t, nil), "--dsn", dsn, "token"}, &stdout, &stderr)
		if code != 2 || !strings.HasPrefix(stderr.String(), "host pair: --dsn: ") ||
			strings.Contains(stderr.String(), "secret") {
			t.Errorf("%s: exit %d, stderr %q", dsn, code, stderr.String())
		}
	}
}

// A snapshot refused for values the target cannot hold is retried on every
// backoff until someone shortens them: the list is shown once, and again only
// when it changed.
func TestRefusedValuesAreListedOnce(t *testing.T) {
	var logged bytes.Buffer
	logger := log.New(&logged, "", 0)
	target := &target{}
	refused := func(total int, items string) error {
		return fmt.Errorf("bootstrap: snapshot: %w", auth.ErrTargetLimits{Total: total, Items: items})
	}
	first := refused(2, "  Film: film.title too long\n  Star: star.mass too large")
	target.showTargetLimits(first, logger)
	target.showTargetLimits(first, logger)
	target.showTargetLimits(errors.New("snapshot: server returned 500"), logger)
	want := "the snapshot is refused for these values of the entity layer:\n" +
		"  Film: film.title too long\n  Star: star.mass too large\n"
	if logged.String() != want {
		t.Errorf("log = %q, want %q", logged.String(), want)
	}
	// The retry line carries the sentence alone.
	if strings.Contains(first.Error(), "\n") {
		t.Errorf("the error spans several lines: %q", first.Error())
	}
	logged.Reset()
	target.showTargetLimits(refused(1, "  Star: star.mass too large"), logger)
	if want := "the snapshot is refused for these values of the entity layer:\n  Star: star.mass too large\n"; logged.String() != want {
		t.Errorf("after one value was fixed: log = %q, want %q", logged.String(), want)
	}
}

func TestRunStopsWhenTheServerAsksForANewerClient(t *testing.T) {
	url := fakeServer(t, map[string]func() (int, string){"exchange": upgradeRequired})
	target, err := newTarget("db-1", &config.Config{ServerURL: url}, "tok", "dsn", false)
	if err != nil {
		t.Fatal(err)
	}
	var logged bytes.Buffer
	connected, err := connectAndPump(context.Background(), newAuthClient(url), target, nil,
		log.New(&logged, "", 0))
	if connected || !eesync.IsPermanent(err) {
		t.Fatalf("connected = %v, err = %v — want a permanent error", connected, err)
	}
	if errors.Is(err, errCredentialRevoked) || err.Error() != "permanent: "+upgradeSentence {
		t.Errorf("message = %q, want the server's sentence", err)
	}
}

// unreachableBaseDSN names a server nothing listens on: provisioning fails at
// once, after everything a claim writes locally.
const unreachableBaseDSN = "postgres://u:p@127.0.0.1:1/"

func hostRunnerFor(t *testing.T, serverURL string) (*hostRunner, *bytes.Buffer) {
	t.Helper()
	isolateConfig(t)
	driver, err := apply.For("postgres")
	if err != nil {
		t.Fatal(err)
	}
	var logged bytes.Buffer
	return &hostRunner{
		ctx:      context.Background(),
		cfg:      &config.HostConfig{HostID: "host-1", ServerURL: serverURL, Dialect: "postgres"},
		driver:   driver,
		refresh:  "eeh_token",
		baseDSN:  unreachableBaseDSN,
		auth:     newAuthClient(serverURL),
		logger:   log.New(&logged, "", 0),
		stdout:   io.Discard,
		children: make(map[string]*managedChild),
		wake:     make(chan struct{}, 1),
	}, &logged
}

func hostExchangeOK() (int, string) {
	return http.StatusOK, `{"access_token": "access", "expires_in": 900}`
}

// A host serves one database type. A registration of another one — assigned
// by hand, or of a type this release does not know — is skipped before the
// claim: no credential is spent and no profile is written (fakeServer fails
// the test on any call).
func TestHostEnsureSkipsARegistrationOfAnotherDatabaseType(t *testing.T) {
	for dialect, label := range map[string]string{"sqlserver": "SQL Server", "oracle": "oracle"} {
		runner, logged := hostRunnerFor(t, fakeServer(t, nil))
		runner.ensure(framing.HostRegistration{
			DatabaseID: "db-1", Name: "Films", Dialect: dialect, Claimable: true,
		})
		want := `registration "Films" targets ` + label + "; this host serves PostgreSQL — skipping\n"
		if logged.String() != want {
			t.Errorf("log = %q, want %q", logged.String(), want)
		}
		if _, _, err := config.LoadProfile("db-1"); !errors.Is(err, config.ErrNotPaired) {
			t.Errorf("%s: a profile was written (err = %v)", dialect, err)
		}
		if len(runner.children) != 0 {
			t.Errorf("%s: a sync loop was started", dialect)
		}
	}
}

func TestHostEnsureSkipsAClaimThatNeedsANewerClient(t *testing.T) {
	url := fakeServer(t, map[string]func() (int, string){
		"host-exchange": hostExchangeOK,
		"claim":         upgradeRequired,
	})
	runner, logged := hostRunnerFor(t, url)
	runner.ensure(framing.HostRegistration{
		DatabaseID: "db-1", Name: "Films", Dialect: "postgres", Claimable: true,
	})
	if want := "Films: " + upgradeSentence + " — skipping"; !strings.Contains(logged.String(), want) {
		t.Errorf("log = %q, want %q", logged.String(), want)
	}
	if _, _, err := config.LoadProfile("db-1"); !errors.Is(err, config.ErrNotPaired) {
		t.Errorf("a profile was written (err = %v)", err)
	}
}

// Needs a superuser DSN in EE_TEST_POSTGRES_DSN (skipped without it): the
// refusal of `host pair` quotes the statement the driver says grants the
// missing right. The login and database it creates are dropped at the end.
func TestHostPairNamesTheMissingGrant(t *testing.T) {
	adminDSN := os.Getenv("EE_TEST_POSTGRES_DSN")
	if adminDSN == "" {
		t.Skip("EE_TEST_POSTGRES_DSN is not set")
	}
	isolateConfig(t)
	ctx := context.Background()
	pg, err := apply.For("postgres")
	if err != nil {
		t.Fatal(err)
	}
	raw := make([]byte, 4)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	name := "ee_t222_go_" + hex.EncodeToString(raw)
	base, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	base.User = url.UserPassword(name, "secret")
	base.Path = "/"
	baseDSN := base.String()
	targetDSN, err := pg.DeriveDSN(baseDSN, name)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pg.Open(adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, stmt := range []string{
			`DROP DATABASE IF EXISTS "` + name + `" WITH (FORCE)`,
			`DROP ROLE IF EXISTS "` + name + `"`,
		} {
			if _, err := admin.ExecContext(context.Background(), stmt); err != nil {
				t.Errorf("cleanup %s: %v", stmt, err)
			}
		}
		_ = admin.Close()
	})
	// A plain login: it exists, and may not create databases.
	if _, err := pg.EnsureDatabase(ctx, targetDSN, adminDSN); err != nil {
		t.Fatal(err)
	}

	server := fakeServer(t, map[string]func() (int, string){
		"host-exchange": func() (int, string) {
			return http.StatusOK, `{"access_token": "access", "host_id": "host-1", "host_name": "laptop"}`
		},
	})
	grant := `grant the create-database right with: ALTER ROLE "` + name + `" CREATEDB`

	var stdout, stderr bytes.Buffer
	code := cmdHostPair([]string{"--server", server, "--dsn", baseDSN, "token"}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("exit %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
	}
	if want := "Preflight: create database MISSING, DDL unknown, DML unknown — " + grant + "\n"; stdout.String() != want {
		t.Errorf("stdout = %q, want %q", stdout.String(), want)
	}
	if want := "host pair: the provisioning identity cannot create databases — " + grant +
		". Alternatively pass --admin-dsn, or re-run with --skip-preflight if the grant is coming.\n"; stderr.String() != want {
		t.Errorf("stderr = %q, want %q", stderr.String(), want)
	}
	if _, err := config.LoadHost(server); !errors.Is(err, config.ErrHostNotPaired) {
		t.Errorf("a refused pairing was saved (err = %v)", err)
	}

	// An admin connection that may not create databases either: the grant is
	// for that login, and --admin-dsn is no longer the way out.
	stdout.Reset()
	stderr.Reset()
	code = cmdHostPair([]string{
		"--server", server, "--dsn", baseDSN, "--admin-dsn", targetDSN, "token",
	}, &stdout, &stderr)
	if want := "host pair: the provisioning identity cannot create databases — " + grant +
		". Re-run with --skip-preflight if the grant is coming.\n"; code != 1 || stderr.String() != want {
		t.Errorf("exit %d, stderr %q, want %q", code, stderr.String(), want)
	}
	if want := "Preflight: create database MISSING, DDL unknown, DML unknown — " +
		"database creation uses --admin-dsn; " + grant + "\n"; stdout.String() != want {
		t.Errorf("stdout = %q, want %q", stdout.String(), want)
	}

	// The same login pairs once the admin connection does the creating.
	stdout.Reset()
	stderr.Reset()
	code = cmdHostPair([]string{
		"--server", server, "--dsn", baseDSN, "--admin-dsn", adminDSN, "token",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
	}
	if !strings.HasPrefix(stdout.String(),
		"Preflight: create database ok, DDL unknown, DML unknown — database creation uses --admin-dsn\n") {
		t.Errorf("stdout = %q", stdout.String())
	}
	pairing, err := config.LoadHost(server)
	if err != nil || pairing.Config.Dialect != "postgres" || pairing.Config.HostName != "laptop" ||
		pairing.BaseDSN != baseDSN || pairing.AdminDSN != adminDSN {
		t.Errorf("pairing = %+v, err = %v", pairing, err)
	}
}

// Pairing with a pasted token goes through the exchange: the token is good,
// the release is not — said in the server's words alone.
// What the operator reads right after "✓ Created database": the login that runs
// the sync may not create databases — the admin connection did — and the line
// that follows says the right is not needed. Both ways a target is opened.
func TestPreflightAfterCreatingTheDatabase(t *testing.T) {
	for _, server := range liveServers {
		t.Run(server.dialect, func(t *testing.T) {
			serverDSN := os.Getenv(server.env)
			if serverDSN == "" {
				t.Skip(server.env + " is not set")
			}
			driver, err := apply.For(server.dialect)
			if err != nil {
				t.Fatal(err)
			}
			adminDSN, err := driver.DeriveDSN(serverDSN, server.maintenance)
			if err != nil {
				t.Fatal(err)
			}
			admin, err := driver.Open(adminDSN)
			if err != nil {
				t.Fatal(err)
			}
			login := "ee_t222_go_" + randomHex(t)
			classic, managed := login+"_run", login+"_host"
			t.Cleanup(func() {
				for _, database := range []string{classic, managed} {
					if _, err := admin.ExecContext(context.Background(), server.drop(database)); err != nil {
						t.Errorf("cleanup of %s: %v", database, err)
					}
				}
				// A login is refused while one of its sessions is still closing.
				var err error
				for range 20 {
					if _, err = admin.ExecContext(context.Background(), server.dropLogin(login)); err == nil {
						break
					}
					time.Sleep(250 * time.Millisecond)
				}
				if err != nil {
					t.Errorf("cleanup of %s: %v", login, err)
				}
				_ = admin.Close()
			})
			base, err := url.Parse(serverDSN)
			if err != nil {
				t.Fatal(err)
			}
			base.User, base.Path = url.UserPassword(login, "Ee_replica_123"), ""
			baseDSN := base.String()
			wantLines := func(stdout, database string) {
				t.Helper()
				created, preflight, found := strings.Cut(stdout, "Preflight Films: ")
				if !found || !strings.HasSuffix(created, "✓ Created database "+database+" for Films\n") {
					t.Fatalf("stdout = %q", stdout)
				}
				preflight, _, _ = strings.Cut(preflight, " — ")
				if !strings.HasPrefix(preflight, "DDL ok, DML ok, ") || strings.Contains(preflight, "create database") ||
					!strings.HasSuffix(strings.TrimSuffix(preflight, "\n"),
						"no create-database right (not needed: the target database exists)") {
					t.Errorf("preflight = %q", preflight)
				}
			}

			t.Run("run --create-missing", func(t *testing.T) {
				isolateConfig(t)
				// The revoked credential ends the sync loop by itself.
				serverURL := fakeServer(t, map[string]func() (int, string){"exchange": revoked})
				if err := config.SaveProfile("db-1", &config.Config{
					DatabaseID: "db-1", DatabaseName: "Films", Dialect: server.dialect, ServerURL: serverURL,
				}, "tok"); err != nil {
					t.Fatal(err)
				}
				dsn, err := driver.DeriveDSN(baseDSN, classic)
				if err != nil {
					t.Fatal(err)
				}
				var stdout, stderr bytes.Buffer
				cmdRun([]string{"--dsn", dsn, "--create-missing", "--admin-dsn", adminDSN},
					&stdout, &stderr, log.New(io.Discard, "", 0))
				if stdout.Len() == 0 {
					t.Fatalf("nothing printed; stderr: %s", stderr.String())
				}
				wantLines(stdout.String(), classic)
			})

			t.Run("host run", func(t *testing.T) {
				var generation, claims atomic.Int64
				claim := claimAt(managed, &generation, &claims)
				runner, logged := hostRunnerFor(t, fakeServer(t, map[string]func() (int, string){
					"exchange":      revoked,
					"host-exchange": hostExchangeOK,
					"claim": func() (int, string) {
						status, body := claim()
						return status, strings.Replace(body, `"dialect": "postgres"`, `"dialect": "`+server.dialect+`"`, 1)
					},
				}))
				runner.driver, runner.baseDSN, runner.adminDSN = driver, baseDSN, adminDSN
				runner.cfg.Dialect = server.dialect
				var stdout bytes.Buffer // written by ensure only, on this goroutine
				runner.stdout = &stdout
				runner.ensure(framing.HostRegistration{
					DatabaseID: "db-1", Name: "Films", Dialect: server.dialect, Claimable: true,
				})
				runner.wg.Wait()
				if stdout.Len() == 0 {
					t.Fatalf("nothing printed; log: %s", logged.String())
				}
				wantLines(stdout.String(), managed)
			})
		})
	}
}

func TestPairSaysWhenTheServerAsksForANewerClient(t *testing.T) {
	isolateConfig(t)
	client := newAuthClient(fakeServer(t, map[string]func() (int, string){"exchange": upgradeRequired}))
	var stdout, stderr bytes.Buffer
	code := runManualPair(client, "token", &stdout, &stderr)
	if code != 1 || stderr.String() != "pair: "+upgradeSentence+"\n" {
		t.Errorf("exit %d, stderr %q", code, stderr.String())
	}
	if profiles, err := config.ListProfiles(); err != nil || len(profiles) != 0 {
		t.Errorf("a refused pairing was saved: %+v, %v", profiles, err)
	}
}

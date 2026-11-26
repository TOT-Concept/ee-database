// Package cli implements the ee-database command-line interface.
//
// Subcommands:
//
//	ee-database pair --server URL [refresh-token]
//	ee-database run [--database NAME|ID | --all] [--dsn DSN] [--save-dsn] [--skip-bootstrap] [--create-missing] [--verbose]
//	ee-database status [--database NAME|ID]
//	ee-database disconnect [--database NAME|ID]
//	ee-database version
//
// Pairings are stored one profile per schema database (internal/config); when
// several profiles exist and none is selected, interactive runs prompt for one
// and non-interactive runs fail with the list.
package cli

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/mattn/go-isatty"
	"golang.org/x/term"

	"github.com/TOT-Concept/ee-database/internal/apply"
	"github.com/TOT-Concept/ee-database/internal/auth"
	"github.com/TOT-Concept/ee-database/internal/config"
	"github.com/TOT-Concept/ee-database/internal/framing"
	eesync "github.com/TOT-Concept/ee-database/internal/sync"
)

// Version aliases the sync package's release version (single source, so the
// hello-frame agent string can never drift from the CLI banner again).
const Version = eesync.Version

// Run dispatches to the appropriate subcommand. Returns the process exit code.
func Run(args []string, stdout, stderr io.Writer) int {
	logger := log.New(stderr, "", log.LstdFlags)

	if len(args) == 0 {
		return cmdRun(nil, stdout, stderr, logger)
	}
	cmd := args[0]
	rest := args[1:]

	switch cmd {
	case "pair":
		return cmdPair(rest, stdout, stderr)
	case "run":
		return cmdRun(rest, stdout, stderr, logger)
	case "host":
		return cmdHost(rest, stdout, stderr, logger)
	case "status":
		return cmdStatus(rest, stdout, stderr)
	case "disconnect":
		return cmdDisconnect(rest, stdout, stderr)
	case "version", "--version", "-v":
		fmt.Fprintln(stdout, "ee-database", Version)
		return 0
	case "help", "--help", "-h":
		printHelp(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n", cmd)
		printHelp(stderr)
		return 2
	}
}

func printHelp(w io.Writer) {
	fmt.Fprintf(w, `ee-database %s — self-hosted apply client for Entity Enricher schema databases

Receives enrichment delta batches over an outbound WebSocket, applies them to
your own database and acknowledges. Your DSN never leaves this machine.
Each paired schema database gets its own local profile, so one machine can
sync several databases.

Source:   https://github.com/TOT-Concept/ee-database
License:  MIT

Usage:
  ee-database pair --server URL                  Pair via the browser (recommended)
  ee-database pair --server URL <refresh-token>  Pair with a token from the Databases page
  ee-database run --dsn DSN                      Connect and apply deltas (default)
  ee-database run --all                          Sync every paired database (saved DSNs)
  ee-database host pair --server URL --dsn DSN <token>
                                                 Pair this machine as a managed sync host
                                                 (one pairing and one database type per
                                                 server; repeatable against several
                                                 backends)
  ee-database host run [--verbose]               Auto-claim, provision and sync every
                                                 database sync assigned to this host,
                                                 on every paired server (--server URL
                                                 to run a single pairing)
  ee-database status                             Show pairing state (all profiles)
  ee-database disconnect                         Forget one pairing's local credentials
  ee-database version                            Print version

Selection flags (run / status / disconnect):
  --database NAME|ID Select a pairing when several databases are paired.
                     Without it, one pairing is implicit, several prompt on a
                     terminal and fail with the list otherwise.

Run flags:
  --dsn DSN          Target database connection string. Its form follows the
                     database type of the sync (postgres | sqlserver):
                       postgres://user:pass@localhost:5432/mydb
                       sqlserver://user:pass@localhost:1433?database=mydb
                     With no DSN at all, run asks on a terminal for its
                     parts (host, port, user, password, database) and
                     offers to save the DSN they make.
  --save-dsn         Store the DSN in the profile dir (mode 0600) for later
                     runs, once the connection succeeds
  --skip-bootstrap   Skip the initial snapshot apply on first run
  --create-missing   Create the target database first when it does not exist.
                     Uses the DSN's own credentials, which then need the
                     server-level create right (PostgreSQL: CREATEDB, through
                     the 'postgres' maintenance database; SQL Server: CREATE
                     ANY DATABASE, through master).
  --admin-dsn DSN    Optional admin connection for --create-missing: also
                     creates the target DSN's login (with its password) when
                     missing, and gives it the new database. The target DSN
                     then needs no create rights of its own.
  --all              Run every paired database concurrently. Each profile must
                     have a saved DSN (--dsn/--save-dsn are rejected).
  --verbose          Log every delta's write shape (per-table upserts, prunes,
                     DDL) and how long it took; 'host run' takes it too.
                     Without it each applied window still reports its
                     per-table shape on one line.

Environment:
  EE_DATABASE_DSN    DSN fallback when --dsn is not given (single-database runs)
`, Version)
}

// === profile selection =======================================================

func stdinIsTTY() bool {
	fd := os.Stdin.Fd()
	return isatty.IsTerminal(fd) || isatty.IsCygwinTerminal(fd)
}

func listProfilesOrExit(stderr io.Writer) ([]config.Profile, int) {
	profiles, err := config.ListProfiles()
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return nil, 1
	}
	if len(profiles) == 0 {
		fmt.Fprintln(stderr, "ee-database is not paired on this machine.")
		fmt.Fprintln(stderr, "Run 'ee-database pair --server URL' first.")
		return nil, 1
	}
	return profiles, -1
}

func describeProfile(p config.Profile) string {
	dialect := p.Config.Dialect
	if driver, err := apply.For(dialect); err == nil {
		dialect = driver.Dialect()
	}
	return fmt.Sprintf("%s (%s) @ %s", p.Label(), dialect, p.Config.ServerURL)
}

// newAuthClient builds the REST client for one server, identified as this
// release on every request.
func newAuthClient(serverURL string) *auth.Client {
	return auth.New(serverURL, eesync.Agent)
}

// matchProfile resolves a --database value: exact id first, then
// case-insensitive name.
func matchProfile(profiles []config.Profile, sel string) (config.Profile, error) {
	for _, p := range profiles {
		if p.Key == sel || p.Config.DatabaseID == sel {
			return p, nil
		}
	}
	var byName []config.Profile
	for _, p := range profiles {
		if strings.EqualFold(p.Config.DatabaseName, sel) {
			byName = append(byName, p)
		}
	}
	switch len(byName) {
	case 1:
		return byName[0], nil
	case 0:
		return config.Profile{}, fmt.Errorf(
			"no paired database matches %q\n%s", sel, profileListing(profiles))
	default:
		return config.Profile{}, fmt.Errorf(
			"%q matches several pairings — use the database id\n%s", sel, profileListing(profiles))
	}
}

func profileListing(profiles []config.Profile) string {
	var b strings.Builder
	b.WriteString("Paired databases:\n")
	for _, p := range profiles {
		fmt.Fprintf(&b, "  %s  [id %s]\n", describeProfile(p), p.Key)
	}
	return b.String()
}

// selectProfile picks one pairing: explicit --database value, implicit single
// profile, interactive prompt on a TTY, or an error listing the choices.
func selectProfile(profiles []config.Profile, sel string, stdout io.Writer) (config.Profile, error) {
	if sel != "" {
		return matchProfile(profiles, sel)
	}
	if len(profiles) == 1 {
		return profiles[0], nil
	}
	if !stdinIsTTY() {
		return config.Profile{}, fmt.Errorf(
			"several databases are paired — pass --database NAME|ID\n%s", profileListing(profiles))
	}
	return promptProfile(profiles, stdout)
}

func promptProfile(profiles []config.Profile, stdout io.Writer) (config.Profile, error) {
	fmt.Fprintln(stdout, "Several databases are paired on this machine:")
	for i, p := range profiles {
		fmt.Fprintf(stdout, "  %d) %s\n", i+1, describeProfile(p))
	}
	line, err := promptLine(stdout, fmt.Sprintf("Select a database [1-%d]: ", len(profiles)))
	if err != nil {
		return config.Profile{}, fmt.Errorf("read selection: %w", err)
	}
	n, err := strconv.Atoi(line)
	if err != nil || n < 1 || n > len(profiles) {
		return config.Profile{}, fmt.Errorf("invalid selection %q", line)
	}
	return profiles[n-1], nil
}

// stdinLines is the one reader of the terminal, so that answers typed ahead
// of a later prompt are not lost in a discarded buffer.
var stdinLines = bufio.NewReader(os.Stdin)

func promptLine(stdout io.Writer, prompt string) (string, error) {
	fmt.Fprint(stdout, prompt)
	line, err := stdinLines.ReadString('\n')
	if err != nil && line == "" {
		return "", err
	}
	return strings.TrimSpace(line), nil
}

// exampleDSN is the target DSN form of a database type, for messages.
func exampleDSN(driver apply.Driver) string {
	return driver.ComposeDSN(apply.DSNParts{
		Host: "localhost", Port: driver.DefaultPort(), User: "user", Password: "password", Database: "mydb",
	})
}

// promptField asks for one connection parameter; Enter keeps def, and a
// parameter without default must be given.
func promptField(stdout io.Writer, label, def string) (string, error) {
	prompt := "  " + label + ": "
	if def != "" {
		prompt = fmt.Sprintf("  %s [%s]: ", label, def)
	}
	for {
		v, err := promptLine(stdout, prompt)
		if err != nil {
			return "", fmt.Errorf("read %s: %w", strings.ToLower(label), err)
		}
		if v == "" {
			v = def
		}
		if v != "" {
			return v, nil
		}
	}
}

// readPassword reads a line without echo. Ctrl-C while it waits restores the
// terminal's echo before quitting.
func readPassword(stdout io.Writer, prompt string) (string, error) {
	fd := int(os.Stdin.Fd())
	state, err := term.GetState(fd)
	if err != nil {
		return "", err
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	defer func() { signal.Stop(sig); close(sig) }()
	go func() {
		if _, ok := <-sig; ok {
			_ = term.Restore(fd, state)
			fmt.Fprintln(stdout)
			os.Exit(130)
		}
	}()
	fmt.Fprint(stdout, prompt)
	password, err := term.ReadPassword(fd)
	fmt.Fprintln(stdout)
	return string(password), err
}

// promptParts asks for the connection parameters a failed connection points
// at — all of them first, and when the failure names none. Each prompt
// offers the last value, so Enter keeps it.
func promptParts(parts *apply.DSNParts, problem apply.ConnectProblem, stdout io.Writer) error {
	all := problem == apply.ProblemUnknown
	var err error
	if all || problem == apply.ProblemEndpoint {
		if parts.Host, err = promptField(stdout, "Host", parts.Host); err != nil {
			return err
		}
		for {
			port, err := promptField(stdout, "Port", strconv.Itoa(parts.Port))
			if err != nil {
				return err
			}
			if n, err := strconv.Atoi(port); err == nil && n > 0 && n < 65536 {
				parts.Port = n
				break
			}
			fmt.Fprintf(stdout, "  %q is not a port number.\n", port)
		}
	}
	if all || problem == apply.ProblemLogin {
		if parts.User, err = promptField(stdout, "User", parts.User); err != nil {
			return err
		}
		prompt := "  Password: "
		if parts.Password != "" {
			prompt = "  Password [unchanged]: "
		}
		password, err := readPassword(stdout, prompt)
		if err != nil {
			return fmt.Errorf("read password: %w", err)
		}
		if password != "" {
			parts.Password = password
		}
	}
	if all || problem == apply.ProblemDatabase {
		if parts.Database, err = promptField(stdout, "Database", parts.Database); err != nil {
			return err
		}
	}
	return nil
}

// promptConnect asks for the connection parameters of a target run without a
// DSN until they connect, asking again only for the ones a failure points at,
// then offers to keep the DSN they make.
func promptConnect(t *target, createMissing bool, adminDSN string, stdout io.Writer) (*sql.DB, error) {
	fmt.Fprintf(stdout, "No DSN for %s. Enter the connection parameters of the %s database to sync\n",
		targetLabel(t), t.driver.Label())
	fmt.Fprintln(stdout, "(Enter keeps the value in brackets, Ctrl-C quits):")
	parts := apply.DSNParts{Host: "localhost", Port: t.driver.DefaultPort(), Database: t.cfg.DatabaseName}
	problem := apply.ProblemUnknown
	for {
		if err := promptParts(&parts, problem, stdout); err != nil {
			return nil, err
		}
		t.dsn = t.driver.ComposeDSN(parts)
		if u, err := url.Parse(t.dsn); err == nil {
			fmt.Fprintf(stdout, "Connecting to %s …\n", u.Redacted())
		}
		db, p, err := connectTarget(context.Background(), t, createMissing, adminDSN, stdout)
		if err == nil {
			fmt.Fprintln(stdout, "✓ Connected.")
			if !t.saveDSN {
				answer, err := promptLine(stdout, "Save this DSN for later runs? [Y/n]: ")
				if err != nil {
					db.Close()
					return nil, fmt.Errorf("read answer: %w", err)
				}
				t.saveDSN = answer == "" || strings.HasPrefix(strings.ToLower(answer), "y")
			}
			return db, nil
		}
		fmt.Fprintf(stdout, "✗ %v\n", err)
		problem = p
	}
}

// connectTarget opens the target (creating it first with --create-missing)
// and pings it. A failure is explained in words and reports the connection
// parameter it points at.
func connectTarget(ctx context.Context, t *target, createMissing bool, adminDSN string, stdout io.Writer) (*sql.DB, apply.ConnectProblem, error) {
	if createMissing {
		ensured, err := t.driver.EnsureDatabase(ctx, t.dsn, adminDSN)
		if err != nil {
			problem, why := apply.DiagnoseConnect(t.driver, err)
			return nil, problem, fmt.Errorf("create-missing: %s", why)
		}
		if ensured.LoginCreated {
			fmt.Fprintf(stdout, "✓ Created login for %s\n", targetLabel(t))
		}
		if ensured.DatabaseCreated {
			fmt.Fprintf(stdout, "✓ Created database %s for %s\n", ensured.Name, targetLabel(t))
		}
	}
	db, err := t.driver.Open(t.dsn)
	if err != nil {
		return nil, apply.ProblemUnknown, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		problem, why := apply.DiagnoseConnect(t.driver, err)
		return nil, problem, fmt.Errorf("cannot connect to the target database: %s", why)
	}
	return db, apply.ProblemUnknown, nil
}

// === pair ====================================================================

func cmdPair(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("pair", flag.ContinueOnError)
	fs.SetOutput(stderr)
	server := fs.String("server", "", "Server base URL (e.g. https://entityenricher.ai)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	serverURL := strings.TrimSpace(*server)
	if serverURL == "" {
		fmt.Fprintln(stderr, "pair: --server is required (e.g. --server https://entityenricher.ai)")
		return 2
	}
	c := newAuthClient(serverURL)
	if fs.NArg() == 0 {
		return runDeviceCodePair(c, stdout, stderr)
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(stderr, "pair: too many arguments")
		return 2
	}
	return runManualPair(c, fs.Arg(0), stdout, stderr)
}

func runManualPair(c *auth.Client, refreshToken string, stdout, stderr io.Writer) int {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// The exchange both validates the token and reports which database it
	// belongs to (older servers omit the metadata → the profile self-heals
	// on the first run).
	resp, err := c.Exchange(ctx, refreshToken)
	var upgrade auth.ErrUpgradeRequired
	if errors.As(err, &upgrade) {
		// The token is fine; this release is too old for the database sync.
		fmt.Fprintf(stderr, "pair: %v\n", err)
		return 1
	}
	if err != nil {
		fmt.Fprintf(stderr, "pair: failed to validate refresh token: %v\n", err)
		return 1
	}
	return persistPairing(
		c, refreshToken, resp.DatabaseID, resp.DatabaseName, resp.Dialect,
		resp.TargetGeneration, stdout, stderr,
	)
}

func runDeviceCodePair(c *auth.Client, stdout, stderr io.Writer) int {
	startCtx, startCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer startCancel()

	dc, err := c.StartDeviceCode(startCtx, "")
	if err != nil {
		fmt.Fprintf(stderr, "pair: %v\n", err)
		return 1
	}

	fmt.Fprintln(stdout)
	fmt.Fprintln(stdout, "Open this URL in your browser to pick the database and confirm pairing:")
	fmt.Fprintln(stdout, "  ", dc.VerificationURIComplete)
	fmt.Fprintln(stdout)
	fmt.Fprintf(stdout, "  Code: %s\n", dc.UserCode)
	fmt.Fprintln(stdout)
	if err := openInBrowser(dc.VerificationURIComplete); err != nil {
		fmt.Fprintln(stdout, "(Could not auto-open the browser. Open the URL above manually.)")
	}
	fmt.Fprintf(stdout, "Waiting for confirmation (expires in %ds)...\n", dc.ExpiresIn)

	interval := time.Duration(dc.Interval) * time.Second
	if interval < time.Second {
		interval = 5 * time.Second
	}
	deadline := time.Now().Add(time.Duration(dc.ExpiresIn) * time.Second)

	for {
		if time.Now().After(deadline) {
			fmt.Fprintln(stderr, "pair: timed out waiting for confirmation")
			return 1
		}
		time.Sleep(interval)

		pollCtx, pollCancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := c.PollDeviceCode(pollCtx, dc.DeviceCode)
		pollCancel()
		if err != nil {
			fmt.Fprintf(stderr, "  (poll error: %v — retrying)\n", err)
			continue
		}
		switch resp.Status {
		case "pending":
			continue
		case "ok":
			fmt.Fprintln(stdout, "✓ Confirmed.")
			return persistPairing(
				c, resp.RefreshToken, resp.DatabaseID, resp.DatabaseName, resp.Dialect,
				resp.TargetGeneration, stdout, stderr,
			)
		case "cancelled":
			fmt.Fprintln(stderr, "pair: the user cancelled the pairing in the browser.")
			return 1
		case "expired":
			fmt.Fprintln(stderr, "pair: the pairing expired. Run 'ee-database pair --server ...' again.")
			return 1
		case "not_found":
			fmt.Fprintln(stderr, "pair: server lost track of the pairing. Try again.")
			return 1
		default:
			fmt.Fprintf(stderr, "pair: unexpected poll status %q\n", resp.Status)
			return 1
		}
	}
}

// targetChanged reports whether the server changed the registration's target
// database since the profile was paired or claimed: the profile's saved DSN
// then names the previous target, which must not receive the new one's feed.
func targetChanged(cfg config.Config, serverGeneration int) bool {
	return cfg.TargetGeneration != serverGeneration
}

// dialectLabel names a database type for the operator ("PostgreSQL", "SQL
// Server"); a dialect this release does not know is shown as the server
// spells it.
func dialectLabel(dialect string) string {
	driver, err := apply.For(dialect)
	if err != nil {
		return dialect
	}
	return driver.Label()
}

func persistPairing(
	c *auth.Client, refreshToken, databaseID, databaseName, dialect string,
	targetGeneration int, stdout, stderr io.Writer,
) int {
	cfg := &config.Config{
		DatabaseID:       databaseID,
		DatabaseName:     databaseName,
		Dialect:          dialect,
		ServerURL:        c.BaseURL,
		TargetGeneration: targetGeneration,
	}
	key := databaseID
	if key == "" {
		key = config.LegacyKey
	}
	// Pairing again rotates the credential and keeps the saved DSN — unless
	// the target database itself changed in between: the DSN then names the
	// previous one.
	retargeted, hadDSN := false, false
	if prev, _, err := config.LoadProfile(key); err == nil && targetChanged(*prev, targetGeneration) {
		retargeted, hadDSN = true, config.LoadProfileDSN(key) != ""
		if err := config.ClearProfileDSN(key); err != nil {
			fmt.Fprintf(stderr, "pair: %v\n", err)
			return 1
		}
	}
	if err := config.SaveProfile(key, cfg, refreshToken); err != nil {
		fmt.Fprintf(stderr, "pair: save: %v\n", err)
		return 1
	}
	dir, _ := config.ProfileDir(key)
	fmt.Fprintf(stdout, "✓ Paired with %s\n", c.BaseURL)
	if databaseName != "" {
		fmt.Fprintf(stdout, "  Database: %s (%s)\n", databaseName, dialect)
	}
	if retargeted {
		next := "Pass the new database's --dsn on the next run."
		if hadDSN {
			next = "The saved DSN was discarded: pass --dsn on the next run."
		}
		fmt.Fprintf(stdout, "  The target database of %q changed (now %s). %s\n",
			databaseName, dialectLabel(dialect), next)
	}
	fmt.Fprintf(stdout, "  Credentials stored in %s\n", dir)
	runHint := "ee-database run --dsn <your-dsn>"
	if databaseID != "" {
		runHint = "ee-database run --database " + databaseID + " --dsn <your-dsn>"
	}
	fmt.Fprintf(stdout, "  Run '%s' to start syncing.\n", runHint)
	return 0
}

// === status / disconnect =====================================================

func cmdStatus(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	database := fs.String("database", "", "Show one pairing (name or id)")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	profiles, err := config.ListProfiles()
	if err != nil {
		fmt.Fprintf(stderr, "status: %v\n", err)
		return 1
	}
	if len(profiles) == 0 {
		fmt.Fprintln(stdout, "Not paired. Run 'ee-database pair --server URL' to pair this machine.")
		return 0
	}
	if *database != "" {
		p, err := matchProfile(profiles, *database)
		if err != nil {
			fmt.Fprintf(stderr, "status: %v\n", err)
			return 1
		}
		profiles = []config.Profile{p}
	}
	fmt.Fprintf(stdout, "ee-database %s — %d paired database(s)\n", Version, len(profiles))
	for _, p := range profiles {
		_, refresh, _ := config.LoadProfile(p.Key)
		dir, _ := config.ProfileDir(p.Key)
		fmt.Fprintf(stdout, "\n%s\n", describeProfile(p))
		fmt.Fprintf(stdout, "  profile dir   %s\n", dir)
		fmt.Fprintf(stdout, "  bootstrapped  %v\n", p.Config.Bootstrapped)
		fmt.Fprintf(stdout, "  dsn saved     %v\n", config.LoadProfileDSN(p.Key) != "")
		fmt.Fprintf(stdout, "  token len     %d (hidden)\n", len(refresh))
	}
	return 0
}

func cmdDisconnect(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("disconnect", flag.ContinueOnError)
	fs.SetOutput(stderr)
	database := fs.String("database", "", "Pairing to forget (name or id)")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	profiles, code := listProfilesOrExit(stderr)
	if code >= 0 {
		return code
	}
	p, err := selectProfile(profiles, *database, stdout)
	if err != nil {
		fmt.Fprintf(stderr, "disconnect: %v\n", err)
		return 1
	}
	if err := config.ClearProfile(p.Key); err != nil {
		fmt.Fprintf(stderr, "disconnect: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "✓ Local credentials for %s removed.\n", p.Label())
	fmt.Fprintln(stdout, "  Note: revoke the credential on the Databases page to fully disable")
	fmt.Fprintln(stdout, "  this machine's access. 'disconnect' only forgets local state.")
	return 0
}

// === run =====================================================================

// target is one profile scheduled for syncing. key mutates when a legacy
// profile self-heals to its real database id.
type target struct {
	key     string
	cfg     *config.Config
	refresh string
	dsn     string
	// driver is the registration's database type, resolved once when the
	// target is built: everything this run opens and applies goes through it.
	driver    apply.Driver
	preflight framing.PreflightReport
	// trigram is lazily built from the preflight on first connect and shared
	// across reconnects, so the missing-pg_trgm advisory fires once per run.
	trigram *eesync.TrigramAdvisor
	// limitsShown is the list of values the last refused snapshot named, so
	// the retries of the same refusal do not print it again.
	limitsShown string
	// verbose adds the per-delta write shape and timing to this target's log.
	verbose bool
	// saveDSN stores dsn in the profile once the target answers, so a DSN
	// that does not connect is never kept.
	saveDSN bool
	// askDSN: no DSN was given or saved, so the run asks for its parameters.
	askDSN bool
}

// newTarget resolves the profile's dialect to its driver; a dialect this
// release cannot serve stops here, before anything is opened.
func newTarget(key string, cfg *config.Config, refresh, dsn string, verbose bool) (*target, error) {
	driver, err := apply.For(cfg.Dialect)
	if err != nil {
		return nil, err
	}
	return &target{
		key: key, cfg: cfg, refresh: refresh, dsn: dsn, driver: driver, verbose: verbose,
	}, nil
}

// cmdRun runs the apply loop(s) until interrupted.
func cmdRun(args []string, stdout, stderr io.Writer, logger *log.Logger) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dsnFlag := fs.String("dsn", "", "Target database connection string")
	saveDSN := fs.Bool("save-dsn", false, "Store the DSN (mode 0600) for later runs")
	skipBootstrap := fs.Bool("skip-bootstrap", false, "Skip the initial snapshot apply")
	createMissing := fs.Bool("create-missing", false, "Create the target database when it does not exist")
	adminDSN := fs.String("admin-dsn", "", "Admin connection used by --create-missing to bootstrap the target login and database")
	database := fs.String("database", "", "Pairing to run (name or id)")
	all := fs.Bool("all", false, "Run every paired database concurrently")
	verbose := fs.Bool("verbose", false, "Log each delta's write shape and timing")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *adminDSN != "" && !*createMissing {
		fmt.Fprintln(stderr, "run: --admin-dsn only applies with --create-missing")
		return 2
	}

	profiles, code := listProfilesOrExit(stderr)
	if code >= 0 {
		return code
	}

	var targets []*target
	if *all {
		if *dsnFlag != "" || *saveDSN {
			fmt.Fprintln(stderr, "run: --dsn/--save-dsn cannot be combined with --all;")
			fmt.Fprintln(stderr, "save each DSN once with 'run --database <x> --dsn <dsn> --save-dsn'.")
			return 2
		}
		if *database != "" {
			fmt.Fprintln(stderr, "run: --database cannot be combined with --all")
			return 2
		}
		var missing []string
		for _, p := range profiles {
			dsn := config.LoadProfileDSN(p.Key)
			if dsn == "" {
				missing = append(missing, p.Label())
				continue
			}
			_, refresh, err := config.LoadProfile(p.Key)
			if err != nil {
				fmt.Fprintf(stderr, "run: %s: %v\n", p.Label(), err)
				return 1
			}
			t, err := newTarget(p.Key, p.Config, refresh, dsn, *verbose)
			if err != nil {
				fmt.Fprintf(stderr, "run: %s: %v\n", p.Label(), err)
				return 1
			}
			targets = append(targets, t)
		}
		if len(missing) > 0 {
			fmt.Fprintf(stderr, "run: no saved DSN for: %s\n", strings.Join(missing, ", "))
			fmt.Fprintln(stderr, "Save each once with 'run --database <x> --dsn <dsn> --save-dsn'.")
			return 2
		}
	} else {
		p, err := selectProfile(profiles, *database, stdout)
		if err != nil {
			fmt.Fprintf(stderr, "run: %v\n", err)
			return 1
		}
		_, refresh, err := config.LoadProfile(p.Key)
		if err != nil {
			fmt.Fprintf(stderr, "run: %v\n", err)
			return 1
		}
		dsn := strings.TrimSpace(*dsnFlag)
		if dsn == "" {
			dsn = os.Getenv("EE_DATABASE_DSN")
		}
		if dsn == "" {
			dsn = config.LoadProfileDSN(p.Key)
		}
		t, err := newTarget(p.Key, p.Config, refresh, dsn, *verbose)
		if err != nil {
			fmt.Fprintf(stderr, "run: %s: %v\n", p.Label(), err)
			return 1
		}
		t.saveDSN = *saveDSN
		if t.dsn == "" {
			if !stdinIsTTY() {
				fmt.Fprintf(stderr, "run: no DSN for %s. Pass --dsn (with --save-dsn to keep it) or set EE_DATABASE_DSN.\n", p.Label())
				fmt.Fprintf(stderr, "  Example: %s\n", exampleDSN(t.driver))
				return 2
			}
			t.askDSN = true
		}
		targets = append(targets, t)
	}

	// Lock every target first, so a sync another process already runs is
	// refused before anything is asked.
	for _, t := range targets {
		unlock, err := config.LockProfile(t.key)
		if err != nil {
			fmt.Fprintf(stderr, "run: %s: %v\n", targetLabel(t), err)
			return 1
		}
		defer unlock()
	}

	// A target without DSN is asked for its parameters before signals are
	// caught, so Ctrl-C still quits the prompt.
	dbs := make([]*sql.DB, len(targets))
	for i, t := range targets {
		if !t.askDSN {
			continue
		}
		db, err := promptConnect(t, *createMissing, *adminDSN, stdout)
		if err != nil {
			fmt.Fprintf(stderr, "run: %s: %v\n", targetLabel(t), err)
			return 1
		}
		defer db.Close()
		dbs[i] = db
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Open and ping every target before starting any loop — a fully
	// misconfigured target should fail the command, not die quietly later.
	for i, t := range targets {
		if *skipBootstrap && !t.cfg.Bootstrapped {
			t.cfg.Bootstrapped = true
			_ = config.SaveProfileConfig(t.key, t.cfg)
		}
		if dbs[i] == nil {
			db, _, err := connectTarget(ctx, t, *createMissing, *adminDSN, stdout)
			if err != nil {
				fmt.Fprintf(stderr, "run: %s: %v\n", targetLabel(t), err)
				return 1
			}
			defer db.Close()
			dbs[i] = db
		}
		db := dbs[i]
		if t.saveDSN {
			if err := config.SaveProfileDSN(t.key, t.dsn); err != nil {
				fmt.Fprintf(stderr, "run: %s: save dsn: %v\n", targetLabel(t), err)
				return 1
			}
			fmt.Fprintf(stdout, "✓ DSN saved for %s; later runs use it.\n", targetLabel(t))
		}
		// Provisioning-rights self-check: printed here, reported to the server
		// on every connect so the Sync client card shows gaps before a publish
		// queues deltas that could never apply. Advisory — a missing right
		// doesn't stop the run; the apply path still surfaces real failures.
		t.preflight = t.driver.Preflight(ctx, t.dsn, db)
		fmt.Fprintf(stdout, "Preflight %s: %s\n", targetLabel(t), preflightSummary(t.preflight, true))
	}

	var wg sync.WaitGroup
	failures := make([]error, len(targets))
	for i, t := range targets {
		targetLogger := logger
		if len(targets) > 1 {
			targetLogger = log.New(stderr, "["+targetLabel(t)+"] ", log.LstdFlags)
		}
		targetLogger.Printf("ee-database %s connecting to %s", Version, t.cfg.ServerURL)
		wg.Add(1)
		go func(i int, t *target, db *sql.DB, lg *log.Logger) {
			defer wg.Done()
			err := runForever(ctx, t, db, lg)
			if err != nil && !errors.Is(err, context.Canceled) {
				lg.Printf("fatal: %v", err)
				failures[i] = err
			} else {
				lg.Println("stopped.")
			}
		}(i, t, dbs[i], targetLogger)
	}
	wg.Wait()

	for _, err := range failures {
		if err != nil {
			return 1
		}
	}
	return 0
}

// failedSnapshotPath resolves the profile-local dump written when a snapshot
// fails to apply; "" (no dump) when the profile directory cannot be resolved.
func failedSnapshotPath(key string) string {
	p, err := config.FailedSnapshotPath(key)
	if err != nil {
		return ""
	}
	return p
}

func targetLabel(t *target) string {
	if t.cfg.DatabaseName != "" {
		return t.cfg.DatabaseName
	}
	return t.key
}

// runForever keeps one target's sync up: dial, pump, on disconnect back off
// and redial.
func runForever(
	ctx context.Context,
	t *target,
	db *sql.DB,
	logger *log.Logger,
) error {
	authClient := newAuthClient(t.cfg.ServerURL)
	backoff := newBackoff()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		connected, err := connectAndPump(ctx, authClient, t, db, logger)
		if connected {
			backoff.reset()
		}
		if errors.Is(err, context.Canceled) {
			return err
		}
		if eesync.IsPermanent(err) {
			return err
		}
		wait := backoff.next()
		logger.Printf("disconnected: %v — retrying in %s", err, wait)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

// errCredentialRevoked marks the exchange 401: the stored refresh token was
// revoked or rotated server-side. A classic runner can only stop and ask for a
// re-pair; a managed child re-claims through its host instead (the claim
// endpoint sanctions re-claim by the assigned host as rotation), so the stale
// profile never dead-ends the sync — see hostRunner.ensure.
var errCredentialRevoked = errors.New("credential revoked or rotated")

func connectAndPump(
	ctx context.Context,
	authClient *auth.Client,
	t *target,
	db *sql.DB,
	logger *log.Logger,
) (bool, error) {
	exchangeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	access, err := authClient.Exchange(exchangeCtx, t.refresh)
	if err != nil {
		var upgrade auth.ErrUpgradeRequired
		if errors.As(err, &upgrade) {
			// Retrying cannot help: only a newer release can serve this sync.
			return false, eesync.PermanentError{Inner: err}
		}
		if strings.Contains(err.Error(), "401") {
			return false, eesync.PermanentError{Inner: fmt.Errorf(
				"%w — run 'ee-database pair' again: %v", errCredentialRevoked, err)}
		}
		return false, err
	}
	healProfile(t, access, logger)

	if t.trigram == nil {
		t.trigram = &eesync.TrigramAdvisor{
			Missing: t.preflight.Trigram != nil && !*t.preflight.Trigram,
		}
	}
	opts := eesync.Options{
		Driver:   t.driver,
		Trigram:  t.trigram,
		FailDump: failedSnapshotPath(t.key),
		Verbose:  t.verbose,
	}

	// One closure serves the first bootstrap, the publish_required deferral and
	// the snapshot_required re-bootstrap (the snapshot is the only carrier of
	// rows written while a schema was unlinked). Re-applying converges:
	// idempotent DDL + ADD COLUMN convergence + revision-guarded upserts.
	bootstrap := func(rctx context.Context) error {
		if err := eesync.Bootstrap(rctx, authClient, access.AccessToken, db, logger,
			opts); err != nil {
			t.showTargetLimits(err, logger)
			return err
		}
		if !t.cfg.Bootstrapped {
			t.cfg.Bootstrapped = true
			if err := config.SaveProfileConfig(t.key, t.cfg); err != nil {
				logger.Printf("warning: could not persist bootstrap flag: %v", err)
			}
		}
		return nil
	}

	// First run: seed the replica from the snapshot before consuming deltas.
	// publish_required is not a failure — no linked schema is published yet, so
	// nothing exists to bootstrap: idle connected and let the first push (the
	// publish queues its bootstrap DDL delta) trigger the deferred snapshot,
	// instead of polling the endpoint on reconnect backoff.
	bootstrapPending := false
	if !t.cfg.Bootstrapped {
		switch err := bootstrap(ctx); {
		case err == nil:
		case errors.Is(err, auth.ErrPublishRequired):
			bootstrapPending = true
			logger.Printf("no linked schema is published yet — staying connected;" +
				" publish from the Database Sync page to start the feed")
		default:
			return false, fmt.Errorf("bootstrap: %w", err)
		}
	}

	conn, err := eesync.Dial(ctx, authClient.WSURL(), access.AccessToken)
	if err != nil {
		return false, err
	}
	defer conn.Close(websocket.StatusNormalClosure, "client exit")
	logger.Printf("connected ✓ waiting for deltas")

	// Provisioning-rights report for the Sync client card (best effort).
	if err := conn.Send(ctx, framing.Preflight{Type: "preflight", Report: t.preflight}); err != nil {
		logger.Printf("warning: could not send preflight report: %v", err)
	}

	return true, eesync.Pump(ctx, conn, db, logger, bootstrap, bootstrapPending, opts)
}

// showTargetLimits lists the values a snapshot was refused for. The refusal
// lasts until someone shortens them on the server and is retried on every
// backoff: the list is printed on first sight and when it changes, and the
// retry line keeps to the error's one sentence.
func (t *target) showTargetLimits(err error, logger *log.Logger) {
	var limits auth.ErrTargetLimits
	if !errors.As(err, &limits) || limits.Items == t.limitsShown {
		return
	}
	t.limitsShown = limits.Items
	logger.Printf("the snapshot is refused for these values of the entity layer:\n%s", limits.Items)
}

// preflightSummary renders one report as a run-banner line. A target is checked
// through a connection to its own database, which therefore exists: creating
// databases is then a right nobody needs, and lacking it is no gap (the Sync
// client card words it the same way). A host's whole role is to create them.
func preflightSummary(r framing.PreflightReport, databaseExists bool) string {
	part := func(name string, v *bool) string {
		switch {
		case v == nil:
			return name + " unknown"
		case *v:
			return name + " ok"
		default:
			return name + " MISSING"
		}
	}
	rights := []string{part("DDL", r.DDL), part("DML", r.DML)}
	if r.Trigram != nil {
		rights = append(rights, part("pg_trgm", r.Trigram))
	}
	if databaseExists && r.CreateDB != nil && !*r.CreateDB {
		rights = append(rights, "no create-database right (not needed: the target database exists)")
	} else {
		rights = append([]string{part("create database", r.CreateDB)}, rights...)
	}
	s := strings.Join(rights, ", ")
	if len(r.Details) > 0 {
		s += " — " + strings.Join(r.Details, "; ")
	}
	return s
}

// healProfile completes a profile whose pairing predates database metadata
// (token paste on an old server, pre-profiles layout): fills the config from
// the exchange response and moves the directory under the real database id.
func healProfile(t *target, access *auth.ExchangeResponse, logger *log.Logger) {
	if access.DatabaseID == "" {
		return
	}
	changed := false
	if t.cfg.DatabaseID != access.DatabaseID {
		t.cfg.DatabaseID = access.DatabaseID
		changed = true
	}
	if access.DatabaseName != "" && t.cfg.DatabaseName != access.DatabaseName {
		t.cfg.DatabaseName = access.DatabaseName
		changed = true
	}
	if t.cfg.Dialect == "" && access.Dialect != "" {
		t.cfg.Dialect = access.Dialect
		changed = true
	}
	if t.key != access.DatabaseID {
		if err := config.RenameProfile(t.key, access.DatabaseID); err != nil {
			logger.Printf("warning: could not move profile %q to %q: %v",
				t.key, access.DatabaseID, err)
		} else {
			t.key = access.DatabaseID
			changed = true
		}
	}
	if changed {
		if err := config.SaveProfileConfig(t.key, t.cfg); err != nil {
			logger.Printf("warning: could not persist profile metadata: %v", err)
		}
	}
}

// === backoff =================================================================

type backoff struct {
	current time.Duration
	max     time.Duration
}

func newBackoff() *backoff {
	return &backoff{current: 1 * time.Second, max: 30 * time.Second}
}

func (b *backoff) next() time.Duration {
	d := b.current
	b.current *= 2
	if b.current > b.max {
		b.current = b.max
	}
	return d
}

func (b *backoff) reset() {
	b.current = 1 * time.Second
}

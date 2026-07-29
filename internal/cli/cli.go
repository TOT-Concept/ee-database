// Package cli implements the ee-database command-line interface.
//
// Subcommands:
//
//	ee-database pair --server URL [refresh-token]
//	ee-database run [--database NAME|ID | --all] [--dsn DSN] [--save-dsn] [--skip-bootstrap] [--create-missing]
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
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/mattn/go-isatty"

	"github.com/TOT-Concept/ee-database/internal/apply"
	"github.com/TOT-Concept/ee-database/internal/auth"
	"github.com/TOT-Concept/ee-database/internal/config"
	eesync "github.com/TOT-Concept/ee-database/internal/sync"
)

const Version = "1.1.0"

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
  ee-database status                             Show pairing state (all profiles)
  ee-database disconnect                         Forget one pairing's local credentials
  ee-database version                            Print version

Selection flags (run / status / disconnect):
  --database NAME|ID Select a pairing when several databases are paired.
                     Without it, one pairing is implicit, several prompt on a
                     terminal and fail with the list otherwise.

Run flags:
  --dsn DSN          Target database connection string, e.g.
                     postgres://user:pass@localhost:5432/mydb
  --save-dsn         Store the DSN in the profile dir (mode 0600) for later runs
  --skip-bootstrap   Skip the initial snapshot apply on first run
  --create-missing   Create the target database first when it does not exist.
                     Uses the DSN's own credentials, which then need
                     server-level create rights (postgres: CREATEDB via the
                     'postgres' maintenance database; mysql: CREATE DATABASE
                     IF NOT EXISTS; sqlite: the file is created anyway).
  --admin-dsn DSN    Optional admin connection for --create-missing: also
                     creates the target DSN's role/user (with its password)
                     when missing, and makes it own the new database. The
                     target DSN then needs no create rights of its own.
  --all              Run every paired database concurrently. Each profile must
                     have a saved DSN (--dsn/--save-dsn are rejected).

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
	if dialect == "" {
		dialect = "postgres"
	}
	return fmt.Sprintf("%s (%s) @ %s", p.Label(), dialect, p.Config.ServerURL)
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
	fmt.Fprintf(stdout, "Select a database [1-%d]: ", len(profiles))
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		return config.Profile{}, fmt.Errorf("read selection: %w", err)
	}
	n, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil || n < 1 || n > len(profiles) {
		return config.Profile{}, fmt.Errorf("invalid selection %q", strings.TrimSpace(line))
	}
	return profiles[n-1], nil
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
	c := auth.New(serverURL)
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
	if err != nil {
		fmt.Fprintf(stderr, "pair: failed to validate refresh token: %v\n", err)
		return 1
	}
	return persistPairing(
		c, refreshToken, resp.DatabaseID, resp.DatabaseName, resp.Dialect,
		stdout, stderr,
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
				stdout, stderr,
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

func persistPairing(
	c *auth.Client, refreshToken, databaseID, databaseName, dialect string,
	stdout, stderr io.Writer,
) int {
	cfg := &config.Config{
		DatabaseID:   databaseID,
		DatabaseName: databaseName,
		Dialect:      dialect,
		ServerURL:    c.BaseURL,
	}
	key := databaseID
	if key == "" {
		key = config.LegacyKey
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
}

// cmdRun runs the apply loop(s) until interrupted.
func cmdRun(args []string, stdout, stderr io.Writer, logger *log.Logger) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dsnFlag := fs.String("dsn", "", "Target database connection string")
	saveDSN := fs.Bool("save-dsn", false, "Store the DSN (mode 0600) for later runs")
	skipBootstrap := fs.Bool("skip-bootstrap", false, "Skip the initial snapshot apply")
	createMissing := fs.Bool("create-missing", false, "Create the target database when it does not exist")
	adminDSN := fs.String("admin-dsn", "", "Admin connection used by --create-missing to bootstrap the target role and database")
	database := fs.String("database", "", "Pairing to run (name or id)")
	all := fs.Bool("all", false, "Run every paired database concurrently")
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
			targets = append(targets, &target{key: p.Key, cfg: p.Config, refresh: refresh, dsn: dsn})
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
		if dsn == "" {
			fmt.Fprintln(stderr, "run: no DSN. Pass --dsn, set EE_DATABASE_DSN, or use --save-dsn once.")
			return 2
		}
		if *saveDSN {
			if err := config.SaveProfileDSN(p.Key, dsn); err != nil {
				fmt.Fprintf(stderr, "run: save dsn: %v\n", err)
				return 1
			}
		}
		targets = append(targets, &target{key: p.Key, cfg: p.Config, refresh: refresh, dsn: dsn})
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Lock, open and ping every target before starting any loop — a fully
	// misconfigured target should fail the command, not die quietly later.
	dbs := make([]*sql.DB, len(targets))
	for i, t := range targets {
		unlock, err := config.LockProfile(t.key)
		if err != nil {
			fmt.Fprintf(stderr, "run: %s: %v\n", targetLabel(t), err)
			return 1
		}
		defer unlock()

		if *skipBootstrap && !t.cfg.Bootstrapped {
			t.cfg.Bootstrapped = true
			_ = config.SaveProfileConfig(t.key, t.cfg)
		}
		if *createMissing {
			ensured, err := apply.EnsureDatabase(ctx, t.cfg.Dialect, t.dsn, *adminDSN)
			if err != nil {
				fmt.Fprintf(stderr, "run: %s: create-missing: %v\n", targetLabel(t), err)
				return 1
			}
			if ensured.RoleCreated {
				fmt.Fprintf(stdout, "✓ Created role for %s\n", targetLabel(t))
			}
			if ensured.DatabaseCreated {
				fmt.Fprintf(stdout, "✓ Created database %s for %s\n", ensured.Name, targetLabel(t))
			}
		}
		db, err := apply.Open(t.cfg.Dialect, t.dsn)
		if err != nil {
			fmt.Fprintf(stderr, "run: %s: %v\n", targetLabel(t), err)
			return 1
		}
		defer db.Close()
		if err := db.PingContext(ctx); err != nil {
			fmt.Fprintf(stderr, "run: %s: cannot reach the target database: %v\n", targetLabel(t), err)
			return 1
		}
		dbs[i] = db
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
	authClient := auth.New(t.cfg.ServerURL)
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
		if strings.Contains(err.Error(), "401") {
			return false, eesync.PermanentError{Inner: fmt.Errorf(
				"credential revoked or rotated — run 'ee-database pair' again: %w", err)}
		}
		return false, err
	}
	healProfile(t, access, logger)

	// First run: seed the replica from the snapshot before consuming deltas.
	if !t.cfg.Bootstrapped {
		if err := eesync.Bootstrap(ctx, authClient, access.AccessToken, db, logger); err != nil {
			return false, fmt.Errorf("bootstrap: %w", err)
		}
		t.cfg.Bootstrapped = true
		if err := config.SaveProfileConfig(t.key, t.cfg); err != nil {
			logger.Printf("warning: could not persist bootstrap flag: %v", err)
		}
	}

	conn, err := eesync.Dial(ctx, authClient.WSURL(), access.AccessToken)
	if err != nil {
		return false, err
	}
	defer conn.Close(websocket.StatusNormalClosure, "client exit")
	logger.Printf("connected ✓ waiting for deltas")

	// The server pauses the feed with snapshot_required after a schema is
	// re-linked (publish model) — the snapshot is the only carrier of rows
	// written while it was unlinked. Re-applying converges: idempotent DDL +
	// ADD COLUMN convergence + revision-guarded upserts.
	rebootstrap := func(rctx context.Context) error {
		return eesync.Bootstrap(rctx, authClient, access.AccessToken, db, logger)
	}
	return true, eesync.Pump(ctx, conn, db, logger, rebootstrap)
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
	if access.Dialect != "" && t.cfg.Dialect != access.Dialect {
		if t.cfg.Dialect != "" {
			logger.Printf("warning: server reports dialect %q but this run opened %q — restart to apply",
				access.Dialect, t.cfg.Dialect)
		}
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

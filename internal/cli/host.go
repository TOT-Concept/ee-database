package cli

// Managed host mode (issue #65): pair a machine once per server, then every
// database sync assigned to it is claimed, provisioned and synced
// automatically.
//
//	ee-database host pair --server URL --dsn BASE_DSN [--admin-dsn DSN] [--skip-preflight] <token>
//	ee-database host run [--server URL]
//	ee-database host status
//	ee-database host disconnect [--server URL]
//
// A machine can hold one pairing per server (dev + prod, staging + prod), and
// a pairing serves one database type, the one its base DSN names; `host run` runs them all — per pairing,
// one control-plane WebSocket (assignment events + host preflight) and one
// ordinary per-database data-plane loop per claimed sync. Managed mode is a
// profile factory: a claim ends by writing exactly the profile a human creates
// with `pair` + `run --save-dsn`, so everything downstream is the same code.

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"

	"github.com/TOT-Concept/ee-database/internal/apply"
	"github.com/TOT-Concept/ee-database/internal/auth"
	"github.com/TOT-Concept/ee-database/internal/config"
	"github.com/TOT-Concept/ee-database/internal/framing"
	eesync "github.com/TOT-Concept/ee-database/internal/sync"
)

func cmdHost(args []string, stdout, stderr io.Writer, logger *log.Logger) int {
	if len(args) == 0 {
		printHostHelp(stderr)
		return 2
	}
	switch args[0] {
	case "pair":
		return cmdHostPair(args[1:], stdout, stderr)
	case "run":
		return cmdHostRun(args[1:], stdout, stderr, logger)
	case "status":
		return cmdHostStatus(stdout, stderr)
	case "disconnect":
		return cmdHostDisconnect(args[1:], stdout, stderr)
	case "help", "--help", "-h":
		printHostHelp(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown host command %q\n\n", args[0])
		printHostHelp(stderr)
		return 2
	}
}

func printHostHelp(w io.Writer) {
	fmt.Fprint(w, `ee-database host — managed mode: pair this machine once per server, sync every assigned database

Usage:
  ee-database host pair --server URL --dsn BASE_DSN [flags] <token>
  ee-database host run [--server URL]
                                Connect the control plane(s) and sync every assigned
                                database. One pairing per server may exist; without
                                --server every pairing runs.
  ee-database host status       Show all host pairings
  ee-database host disconnect [--server URL]
                                Forget one host pairing (revoke server-side in the UI).
                                --server is required when several servers are paired.

Pair flags:
  --server URL       Entity Enricher base URL (e.g. https://entityenricher.ai)
  --dsn DSN          Base server DSN, NO database name:
                       postgres://ee_replica:pw@localhost:5432/
                       sqlserver://ee_replica:pw@localhost:1433
                     Its scheme names the database type the host serves. One
                     pairing serves one database type per server: database syncs
                     of the other type are skipped, and pairing the same server
                     with the other type needs 'host disconnect' first.
                     Stored locally (0600); never sent to the server. Each assigned
                     registration derives its own DSN from it + the server-suggested
                     snake_cased name (override per registration via database_names
                     in the host config.json).
  --admin-dsn DSN    Optional provisioning connection (creates databases and the
                     base DSN's login), used only when creating databases; never
                     stored server-side.
  --skip-preflight   Pair even when the provisioning-rights check fails.

The token comes from the Sync hosts dialog (Database Sync page → "Sync hosts"
toolbar button) → Add host — and is shown once.
`)
}

// === host pair ===============================================================

func cmdHostPair(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("host pair", flag.ContinueOnError)
	fs.SetOutput(stderr)
	server := fs.String("server", "", "Server base URL")
	dsn := fs.String("dsn", "", "Base server DSN (no database name)")
	adminDSN := fs.String("admin-dsn", "", "Optional provisioning DSN")
	skipPreflight := fs.Bool("skip-preflight", false, "Pair even when the rights check fails")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	serverURL := strings.TrimSpace(*server)
	baseDSN := strings.TrimSpace(*dsn)
	admin := strings.TrimSpace(*adminDSN)
	switch {
	case serverURL == "":
		fmt.Fprintln(stderr, "host pair: --server is required")
		return 2
	case baseDSN == "":
		fmt.Fprintln(stderr, "host pair: --dsn is required (base server DSN, no database name)")
		return 2
	case fs.NArg() != 1:
		fmt.Fprintln(stderr, "host pair: exactly one <token> argument is required "+
			"(issued on the Database Sync page, Sync hosts card)")
		return 2
	}
	// Before the token is spent: a base DSN no driver can derive a database
	// DSN from would pair a host that can serve nothing.
	driver, err := apply.ForDSN(baseDSN)
	if err == nil {
		err = checkBaseDSN(driver, baseDSN)
	}
	if err != nil {
		fmt.Fprintf(stderr, "host pair: --dsn: %v\n", err)
		return 2
	}

	c := newAuthClient(serverURL)
	prev, err := config.LoadHost(c.BaseURL)
	if errors.Is(err, config.ErrHostNotPaired) {
		prev = nil
	} else if err != nil {
		fmt.Fprintf(stderr, "host pair: %v\n", err)
		return 1
	}
	if prev != nil {
		// A pairing of the same type is replaced below. One of the other type
		// is not: the databases it claimed live on a server this base DSN
		// does not reach. (A type this release no longer knows claimed none.)
		if served, err := apply.For(prev.Config.Dialect); err == nil && served.Dialect() != driver.Dialect() {
			fmt.Fprintf(stderr, "host pair: this machine is already paired with %s as a %s host (%q). "+
				"One pairing serves one database type per server; run "+
				"'ee-database host disconnect --server %s' first.\n",
				c.BaseURL, served.Label(), prev.Config.HostName, c.BaseURL)
			return 1
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := c.HostExchange(ctx, fs.Arg(0))
	if err != nil {
		fmt.Fprintf(stderr, "host pair: failed to validate token: %v\n", err)
		return 1
	}

	// Fail fast while the operator is at the terminal: pairing a host that
	// can't provision is a trap that springs later on someone else's screen.
	report := driver.HostPreflight(ctx, baseDSN, admin)
	fmt.Fprintf(stdout, "Preflight: %s\n", preflightSummary(report, false))
	if report.CreateDB != nil && !*report.CreateDB && !*skipPreflight {
		// The details end with the driver's own grant statement; what comes
		// before it is on the Preflight line.
		wayOut := "Alternatively pass --admin-dsn, or re-run"
		if admin != "" {
			wayOut = "Re-run"
		}
		fmt.Fprintf(stderr, "host pair: the provisioning identity cannot create databases — "+
			"%s. %s with --skip-preflight if the grant is coming.\n",
			report.Details[len(report.Details)-1], wayOut)
		return 1
	}

	if prev != nil {
		fmt.Fprintf(stdout, "Replacing this machine's existing pairing with %s (host %q)\n",
			c.BaseURL, prev.Config.HostName)
	}
	cfg := &config.HostConfig{
		HostID:    resp.HostID,
		HostName:  resp.HostName,
		ServerURL: c.BaseURL,
		Dialect:   driver.Dialect(),
	}
	if err := config.SaveHost(cfg, fs.Arg(0), baseDSN, admin); err != nil {
		fmt.Fprintf(stderr, "host pair: save: %v\n", err)
		return 1
	}
	dir, _ := config.HostPairingDir(c.BaseURL)
	fmt.Fprintf(stdout, "✓ Paired as sync host %q with %s\n", resp.HostName, c.BaseURL)
	fmt.Fprintf(stdout, "  State stored in %s\n", dir)
	if pairings, err := config.LoadHosts(); err == nil && len(pairings) > 1 {
		others := make([]string, 0, len(pairings)-1)
		for _, p := range pairings {
			if p.Config.ServerURL != c.BaseURL {
				others = append(others, p.Config.ServerURL)
			}
		}
		fmt.Fprintf(stdout, "  Also paired with %s — one 'host run' syncs every paired server.\n",
			strings.Join(others, ", "))
	}
	fmt.Fprintln(stdout, "  Run 'ee-database host run' to start syncing assigned databases.")
	return 0
}

// checkBaseDSN reports why the driver cannot read a host's base DSN. Only the
// DSN's form is checked — Open does not connect — through the DSN of a
// database derived from it, which is what every claim will open.
func checkBaseDSN(driver apply.Driver, baseDSN string) error {
	probe, err := driver.DeriveDSN(baseDSN, "ee_probe")
	if err != nil {
		return err
	}
	db, err := driver.Open(probe)
	if err != nil {
		return err
	}
	return db.Close()
}

// === host status / disconnect ===============================================

func cmdHostStatus(stdout, stderr io.Writer) int {
	pairings, err := config.LoadHosts()
	if err != nil {
		fmt.Fprintf(stderr, "host status: %v\n", err)
		return 1
	}
	if len(pairings) == 0 {
		fmt.Fprintln(stdout, "Not paired as a sync host. Run 'ee-database host pair' first.")
		return 0
	}
	for _, p := range pairings {
		cfg := p.Config
		dir, _ := config.HostPairingDir(cfg.ServerURL)
		fmt.Fprintf(stdout, "Sync host %q (%s) @ %s\n", cfg.HostName, cfg.Dialect, cfg.ServerURL)
		fmt.Fprintf(stdout, "  state dir     %s\n", dir)
		fmt.Fprintf(stdout, "  base dsn      saved (%d chars)\n", len(p.BaseDSN))
		fmt.Fprintf(stdout, "  admin dsn     %v\n", p.AdminDSN != "")
		fmt.Fprintf(stdout, "  token len     %d (hidden)\n", len(p.Refresh))
		if len(cfg.DatabaseNames) > 0 {
			fmt.Fprintf(stdout, "  name overrides %v\n", cfg.DatabaseNames)
		}
	}
	return 0
}

func cmdHostDisconnect(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("host disconnect", flag.ContinueOnError)
	fs.SetOutput(stderr)
	server := fs.String("server", "", "Server URL of the pairing to forget")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	pairings, err := config.LoadHosts()
	if err != nil {
		fmt.Fprintf(stderr, "host disconnect: %v\n", err)
		return 1
	}
	if len(pairings) == 0 {
		fmt.Fprintln(stdout, "Not paired as a sync host — nothing to disconnect.")
		return 0
	}
	target := strings.TrimSpace(*server)
	if target == "" {
		if len(pairings) > 1 {
			fmt.Fprintln(stderr, "host disconnect: several servers are paired; pick one with --server:")
			for _, p := range pairings {
				fmt.Fprintf(stderr, "  %s\n", p.Config.ServerURL)
			}
			return 2
		}
		target = pairings[0].Config.ServerURL
	}
	normalized := newAuthClient(target).BaseURL
	if _, err := config.LoadHost(normalized); errors.Is(err, config.ErrHostNotPaired) {
		fmt.Fprintf(stderr, "host disconnect: no pairing with %s\n", normalized)
		return 1
	}
	if err := config.ClearHost(normalized); err != nil {
		fmt.Fprintf(stderr, "host disconnect: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "✓ Host pairing with %s removed locally.\n", normalized)
	fmt.Fprintln(stdout, "  Note: revoke the host on the Database Sync page to fully disable")
	fmt.Fprintln(stdout, "  this machine's access (that also revokes its claimed credentials).")
	return 0
}

// === host run ================================================================

func cmdHostRun(args []string, stdout, stderr io.Writer, logger *log.Logger) int {
	fs := flag.NewFlagSet("host run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	server := fs.String("server", "", "Only run the pairing with this server URL")
	verbose := fs.Bool("verbose", false, "Log each delta's write shape and timing")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	var pairings []*config.HostPairing
	if s := strings.TrimSpace(*server); s != "" {
		pairing, err := config.LoadHost(newAuthClient(s).BaseURL)
		if err != nil {
			fmt.Fprintf(stderr, "host run: %v\n", err)
			return 1
		}
		pairings = []*config.HostPairing{pairing}
	} else {
		var err error
		pairings, err = config.LoadHosts()
		if err != nil {
			fmt.Fprintf(stderr, "host run: %v\n", err)
			return 1
		}
		if len(pairings) == 0 {
			fmt.Fprintf(stderr, "host run: %v\n", config.ErrHostNotPaired)
			return 1
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// One runner per paired server, each behind its own pairing lock. A held
	// lock means that server's runner is alive in another process — skip it
	// rather than fail the ones that can start.
	var wg sync.WaitGroup
	var exitMu sync.Mutex
	exit, started := 0, 0
	for _, pairing := range pairings {
		unlock, err := config.LockHostPairing(pairing.Config.ServerURL)
		if err != nil {
			logger.Printf("%s: %v — skipping", pairing.Config.ServerURL, err)
			continue
		}
		started++
		hostLogger := logger
		prefix := ""
		if len(pairings) > 1 {
			prefix = "[" + pairing.Slug + "] "
			hostLogger = log.New(os.Stderr, prefix, log.LstdFlags)
		}
		wg.Add(1)
		go func(p *config.HostPairing) {
			defer wg.Done()
			defer unlock()
			code := runHostPairing(ctx, p, prefix, stdout, hostLogger, *verbose)
			exitMu.Lock()
			if code > exit {
				exit = code
			}
			exitMu.Unlock()
		}(pairing)
	}
	if started == 0 {
		fmt.Fprintln(stderr, "host run: every paired server is already being run by another process")
		return 1
	}
	wg.Wait()
	return exit
}

// runHostPairing runs one server's control-plane loop until cancel or a
// permanent error. Other pairings keep running either way.
func runHostPairing(ctx context.Context, pairing *config.HostPairing,
	prefix string, stdout io.Writer, logger *log.Logger, verbose bool) int {
	cfg := pairing.Config
	driver, err := apply.For(cfg.Dialect)
	if err != nil {
		logger.Printf("fatal: %v — run 'ee-database host pair' again", err)
		return 1
	}
	// Re-check and report, never exit: a host that lost the create-database
	// right still syncs its already-claimed databases correctly (their
	// ownership is intact); the degraded report drops it from auto-assign
	// eligibility server-side.
	report := driver.HostPreflight(ctx, pairing.BaseDSN, pairing.AdminDSN)
	fmt.Fprintf(stdout, "%sPreflight (host): %s\n", prefix, preflightSummary(report, false))

	runner := &hostRunner{
		ctx:       ctx,
		cfg:       cfg,
		driver:    driver,
		refresh:   pairing.Refresh,
		baseDSN:   pairing.BaseDSN,
		adminDSN:  pairing.AdminDSN,
		preflight: report,
		auth:      newAuthClient(cfg.ServerURL),
		logger:    logger,
		logPrefix: prefix,
		stdout:    stdout,
		verbose:   verbose,
		children:  make(map[string]*managedChild),
		wake:      make(chan struct{}, 1),
	}
	logger.Printf("ee-database %s host %q connecting to %s", Version, cfg.HostName, cfg.ServerURL)
	err = runner.runForever()
	runner.stopAll()
	runner.wg.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Printf("fatal: %v", err)
		return 1
	}
	logger.Println("stopped.")
	return 0
}

type managedChild struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// retarget is a registration whose sync found by itself — from the claim that
// follows a revoked credential — that the target database changed.
type retarget struct {
	reg  framing.HostRegistration // the registration, at the claim's generation
	from int                      // the generation of the profile that sync ran on
}

// hostRunner holds the control-plane loop plus one data-plane child per
// claimed database. Children live on the OUTER context, so a control-plane
// reconnect never interrupts running syncs — the reconcile list on the next
// hello trues them up.
type hostRunner struct {
	ctx       context.Context
	cfg       *config.HostConfig
	driver    apply.Driver // the database type of the base DSN
	refresh   string
	baseDSN   string
	adminDSN  string
	preflight framing.PreflightReport
	auth      *auth.Client
	logger    *log.Logger
	logPrefix string // "[slug] " when several pairings run in one process
	stdout    io.Writer
	verbose   bool // --verbose, handed to every managed child's apply log

	mu       sync.Mutex
	children map[string]*managedChild
	// retargets are handed over by the children and taken by the control
	// loop, the one caller of ensure; wake tells it that some are waiting.
	retargets []retarget
	wake      chan struct{}
	wg        sync.WaitGroup
}

func (r *hostRunner) runForever() error {
	backoff := newBackoff()
	for {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		connected, err := r.connectAndPump()
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
		r.logger.Printf("control plane disconnected: %v — retrying in %s", err, wait)
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-time.After(wait):
		}
	}
}

func (r *hostRunner) connectAndPump() (bool, error) {
	exchangeCtx, cancel := context.WithTimeout(r.ctx, 15*time.Second)
	defer cancel()
	access, err := r.auth.HostExchange(exchangeCtx, r.refresh)
	if err != nil {
		if strings.Contains(err.Error(), "401") {
			return false, eesync.PermanentError{Inner: fmt.Errorf(
				"host credential revoked — run 'ee-database host pair' again: %w", err)}
		}
		return false, err
	}
	conn, err := eesync.DialHost(r.ctx, r.auth.HostWSURL(), access.AccessToken, r.driver.Dialect())
	if err != nil {
		return false, err
	}
	defer conn.Close(websocket.StatusNormalClosure, "client exit")
	r.logger.Printf("control plane connected ✓ waiting for assignments")

	if err := conn.Send(r.ctx, framing.Preflight{Type: "preflight", Report: r.preflight}); err != nil {
		r.logger.Printf("warning: could not send host preflight: %v", err)
	}

	pumpCtx, pumpCancel := context.WithCancel(r.ctx)
	defer pumpCancel()
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-pumpCtx.Done():
				return
			case <-ticker.C:
				_ = conn.Send(pumpCtx, framing.Ping{Type: "ping"})
			}
		}
	}()

	// The socket is read on its own goroutine: this loop also listens to the
	// children, and stays the only place a registration is started from.
	frames := make(chan any)
	recvErr := make(chan error, 1)
	go func() {
		for {
			frame, err := conn.RecvHost(pumpCtx)
			if err != nil {
				recvErr <- err
				return
			}
			select {
			case frames <- frame:
			case <-pumpCtx.Done():
				return
			}
		}
	}()

	for {
		select {
		case err := <-recvErr:
			if r.ctx.Err() != nil {
				return true, r.ctx.Err()
			}
			return true, err
		case <-r.wake:
			r.restartRetargeted()
		case frame := <-frames:
			switch f := frame.(type) {
			case framing.HostAssigned:
				r.reconcile(f.Registrations)
			case framing.HostRegistrationAssigned:
				r.ensure(f.Registration)
			case framing.HostRegistrationRevoked:
				r.stop(f.DatabaseID, "assignment revoked")
			case framing.Pong:
				// Heartbeat reply; nothing to do.
			default:
				r.logger.Printf("unexpected control frame: %T", frame)
			}
		}
	}
}

// restartRetargeted starts over, on its new target, each registration whose
// sync stopped by itself because the target database changed: nothing else
// would, when the server's notice of the change was lost. A profile that is no
// longer the one that sync ran on was replaced in the meantime — the notice
// did arrive — and is left alone.
func (r *hostRunner) restartRetargeted() {
	r.mu.Lock()
	pending := r.retargets
	r.retargets = nil
	r.mu.Unlock()
	for _, p := range pending {
		cfgP, _, err := config.LoadProfile(p.reg.DatabaseID)
		if err != nil || cfgP.TargetGeneration != p.from {
			continue
		}
		r.ensure(p.reg)
	}
}

// reconcile trues the children up against the authoritative assigned list.
func (r *hostRunner) reconcile(regs []framing.HostRegistration) {
	want := make(map[string]bool, len(regs))
	for _, reg := range regs {
		want[reg.DatabaseID] = true
	}
	r.mu.Lock()
	var stale []string
	for id := range r.children {
		if !want[id] {
			stale = append(stale, id)
		}
	}
	r.mu.Unlock()
	for _, id := range stale {
		r.stop(id, "no longer assigned")
	}
	for _, reg := range regs {
		r.ensure(reg)
	}
}

// ensure claims, provisions and starts one registration's data-plane loop.
// Failures are logged and skipped — the next reconcile retries them.
func (r *hostRunner) ensure(reg framing.HostRegistration) {
	// Checked before any claim: a registration of the other database type
	// (assigned to this host by hand) must not leave a credential spent and a
	// profile this host cannot serve.
	if serves, err := apply.For(reg.Dialect); err != nil || serves.Dialect() != r.driver.Dialect() {
		r.logger.Printf("registration %q targets %s; this host serves %s — skipping",
			reg.Name, dialectLabel(reg.Dialect), r.driver.Label())
		return
	}

	cfgP, refreshP, err := config.LoadProfile(reg.DatabaseID)
	claimed := err == nil
	if claimed && targetChanged(*cfgP, reg.TargetGeneration) {
		// The registration was pointed at another database since this profile
		// was made: its credential is revoked and its DSN names the previous
		// target. Start over — a fresh claim derives the new database's name.
		r.logger.Printf("%s: the target database changed — claiming it afresh", reg.Name)
		r.stop(reg.DatabaseID, "target database changed")
		if err := config.ClearProfile(reg.DatabaseID); err != nil {
			r.logger.Printf("%s: %v", reg.Name, err)
			return
		}
		claimed = false
	}

	r.mu.Lock()
	_, running := r.children[reg.DatabaseID]
	r.mu.Unlock()
	if running {
		return
	}

	var claim *auth.ClaimResponse
	if !claimed {
		// Not claimed yet on this machine.
		if !reg.Claimable {
			r.logger.Printf("%s: another client is already paired — skipping "+
				"(revoke its credential or move the sync under this host)", reg.Name)
			return
		}
		claim, err = r.claim(reg.DatabaseID)
		if err != nil {
			var notClaimable auth.ErrNotClaimable
			var upgrade auth.ErrUpgradeRequired
			if errors.As(err, &notClaimable) || errors.As(err, &upgrade) {
				r.logger.Printf("%s: %v — skipping", reg.Name, err)
			} else {
				r.logger.Printf("%s: claim failed: %v (retried on next reconcile)", reg.Name, err)
			}
			return
		}
		cfgP = &config.Config{
			DatabaseID:       claim.DatabaseID,
			DatabaseName:     claim.DatabaseName,
			Dialect:          claim.Dialect,
			ServerURL:        r.cfg.ServerURL,
			TargetGeneration: claim.TargetGeneration,
		}
		name := claim.SuggestedDatabaseName
		if override := r.cfg.DatabaseNames[reg.DatabaseID]; override != "" {
			name = override
		}
		// Derived before anything is written: a profile without a DSN would
		// be taken for a claimed one on every later reconcile.
		dsn, err := r.driver.DeriveDSN(r.baseDSN, name)
		if err != nil {
			r.logger.Printf("%s: derive dsn: %v", reg.Name, err)
			return
		}
		if err := config.SaveProfile(reg.DatabaseID, cfgP, claim.RefreshToken); err != nil {
			r.logger.Printf("%s: save profile: %v", reg.Name, err)
			return
		}
		refreshP = claim.RefreshToken
		if err := config.SaveProfileDSN(reg.DatabaseID, dsn); err != nil {
			r.logger.Printf("%s: save dsn: %v", reg.Name, err)
			return
		}
		r.logger.Printf("claimed %s → local database %q", reg.Name, name)
	}

	dsn := config.LoadProfileDSN(reg.DatabaseID)
	if dsn == "" {
		r.logger.Printf("%s: profile has no saved DSN — run "+
			"'ee-database run --database %s --dsn <dsn> --save-dsn' once", reg.Name, reg.DatabaseID)
		return
	}
	unlock, err := config.LockProfile(reg.DatabaseID)
	if err != nil {
		r.logger.Printf("%s: %v — skipping (a classic runner holds it?)", reg.Name, err)
		return
	}

	ensured, err := r.driver.EnsureDatabase(r.ctx, dsn, r.adminDSN)
	if err != nil {
		r.logger.Printf("%s: create-missing: %v (retried on next reconcile)", reg.Name, err)
		unlock()
		return
	}
	if ensured.LoginCreated {
		fmt.Fprintf(r.stdout, "%s✓ Created login for %s\n", r.logPrefix, reg.Name)
	}
	if ensured.DatabaseCreated {
		fmt.Fprintf(r.stdout, "%s✓ Created database %s for %s\n", r.logPrefix, ensured.Name, reg.Name)
	}
	db, err := r.driver.Open(dsn)
	if err != nil {
		r.logger.Printf("%s: %v", reg.Name, err)
		unlock()
		return
	}
	if err := db.PingContext(r.ctx); err != nil {
		r.logger.Printf("%s: cannot reach the target database: %v", reg.Name, err)
		db.Close()
		unlock()
		return
	}

	t := &target{
		key: reg.DatabaseID, cfg: cfgP, refresh: refreshP, dsn: dsn, driver: r.driver,
		verbose: r.verbose,
	}
	t.preflight = r.driver.Preflight(r.ctx, dsn, db)
	fmt.Fprintf(r.stdout, "%sPreflight %s: %s\n", r.logPrefix, targetLabel(t), preflightSummary(t.preflight, true))
	// Unconditional in host mode (the content-gated advisory stays too): the
	// registrations assigned to this host change over time, so a replica that
	// carries no search index today may be handed one tomorrow — the operator
	// hears about the missing extension at claim time, not mid-feed.
	if t.preflight.Trigram != nil && !*t.preflight.Trigram {
		r.logger.Printf("%s: warning: pg_trgm extension not installed — database search "+
			"indexes will no-op until a database owner runs 'CREATE EXTENSION pg_trgm;' "+
			"on this replica", reg.Name)
	}

	childCtx, childCancel := context.WithCancel(r.ctx)
	child := &managedChild{cancel: childCancel, done: make(chan struct{})}
	r.mu.Lock()
	r.children[reg.DatabaseID] = child
	r.mu.Unlock()

	childLogger := log.New(os.Stderr, r.logPrefix+"["+targetLabel(t)+"] ", log.LstdFlags)
	childLogger.Printf("syncing (managed)")
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer close(child.done)
		generation, retargeted := r.syncManaged(childCtx, t, db, childLogger)
		db.Close()
		unlock()
		r.mu.Lock()
		if r.children[reg.DatabaseID] == child {
			delete(r.children, reg.DatabaseID)
		}
		if retargeted {
			// Handed to the control loop, which discards this profile and
			// claims afresh: the claim that revealed the change is this
			// host's, so the registration is its to claim.
			next := reg
			next.TargetGeneration, next.Claimable = generation, true
			r.retargets = append(r.retargets, retarget{reg: next, from: cfgP.TargetGeneration})
		}
		r.mu.Unlock()
		if retargeted {
			select {
			case r.wake <- struct{}{}:
			default: // already told
			}
		}
	}()
}

// syncManaged runs one claimed registration's data plane until it stops. It
// reports true, with the registration's current generation, when the
// registration turned out to target another database than the one this
// profile was claimed for: the profile is then the previous target's, and the
// registration has to be claimed afresh.
func (r *hostRunner) syncManaged(ctx context.Context, t *target, db *sql.DB, logger *log.Logger) (int, bool) {
	err := runForever(ctx, t, db, logger)
	if errors.Is(err, errCredentialRevoked) && ctx.Err() == nil {
		// The stored token died out from under this profile — a previous
		// classic pairing, a claim minted by a since-revoked host, or a change
		// of the target database. The claim endpoint sanctions re-claim by the
		// assigned host as rotation, so the stale credential is disposable:
		// mint a fresh one. One attempt per start — a second 401 means it was
		// revoked again mid-run.
		logger.Printf("credential revoked — re-claiming via host")
		claim, cerr := r.claim(t.key)
		var notClaimable auth.ErrNotClaimable
		var upgrade auth.ErrUpgradeRequired
		switch {
		case errors.As(cerr, &notClaimable):
			logger.Printf("stopped: %v — revoke the other client's "+
				"credential first", cerr)
			return 0, false
		case errors.As(cerr, &upgrade):
			logger.Printf("stopped: %v", cerr)
			return 0, false
		case cerr != nil:
			logger.Printf("stopped: re-claim failed: %v "+
				"(retried on next reconcile)", cerr)
			return 0, false
		}
		if targetChanged(*t.cfg, claim.TargetGeneration) {
			// Not a rotation: the saved DSN and the open connection are the
			// previous target's, and resuming would bootstrap the new feed
			// into it. This runs when the 401 beats the control plane's own
			// notice of the change (socket down, notice lost).
			logger.Printf("stopped: the target database changed — starting over on the new one")
			return claim.TargetGeneration, true
		}
		// A plain rotation: the profile's saved DSN and bootstrap flag are
		// kept, so the local database and its state survive.
		if serr := config.SaveProfile(t.key, t.cfg, claim.RefreshToken); serr != nil {
			// The rotated token still drives this run; the next start
			// lands back here and re-claims again.
			logger.Printf("warning: could not persist rotated token: %v", serr)
		}
		t.refresh = claim.RefreshToken
		logger.Printf("re-claimed — resuming sync")
		err = runForever(ctx, t, db, logger)
		if errors.Is(err, errCredentialRevoked) {
			logger.Printf("stopped: credential revoked again after " +
				"re-claim — retried on next reconcile")
			return 0, false
		}
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Printf("stopped: %v", err)
	} else {
		logger.Println("stopped.")
	}
	return 0, false
}

func (r *hostRunner) claim(databaseID string) (*auth.ClaimResponse, error) {
	ctx, cancel := context.WithTimeout(r.ctx, 15*time.Second)
	defer cancel()
	access, err := r.auth.HostExchange(ctx, r.refresh)
	if err != nil {
		return nil, err
	}
	return r.auth.Claim(ctx, access.AccessToken, databaseID)
}

func (r *hostRunner) stop(databaseID, reason string) {
	r.mu.Lock()
	child := r.children[databaseID]
	delete(r.children, databaseID)
	r.mu.Unlock()
	if child == nil {
		return
	}
	r.logger.Printf("stopping %s: %s", databaseID, reason)
	child.cancel()
	<-child.done
}

func (r *hostRunner) stopAll() {
	r.mu.Lock()
	children := make([]*managedChild, 0, len(r.children))
	for _, c := range r.children {
		children = append(children, c)
	}
	r.children = make(map[string]*managedChild)
	r.mu.Unlock()
	for _, c := range children {
		c.cancel()
		<-c.done
	}
}

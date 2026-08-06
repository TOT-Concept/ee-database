package cli

// Managed host mode (issue #65): pair a machine once, then every database sync
// assigned to it is claimed, provisioned and synced automatically.
//
//	ee-database host pair --server URL --dsn BASE_DSN [--admin-dsn DSN] [--dialect D] [--skip-preflight] <token>
//	ee-database host run
//	ee-database host status
//	ee-database host disconnect
//
// `host run` holds one control-plane WebSocket (assignment events + host
// preflight) and one ordinary per-database data-plane loop per claimed sync —
// managed mode is a profile factory: a claim ends by writing exactly the
// profile a human creates with `pair` + `run --save-dsn`, so everything
// downstream is the same code.

import (
	"context"
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
		return cmdHostDisconnect(stdout, stderr)
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
	fmt.Fprint(w, `ee-database host — managed mode: pair this machine once, sync every assigned database

Usage:
  ee-database host pair --server URL --dsn BASE_DSN [flags] <token>
  ee-database host run          Connect the control plane and sync every assigned database
  ee-database host status       Show the host pairing
  ee-database host disconnect   Forget the host pairing (revoke server-side in the UI)

Pair flags:
  --server URL       Entity Enricher base URL (e.g. https://entityenricher.ai)
  --dsn DSN          Base server DSN, NO database name (e.g. postgres://ee_replica:pw@localhost:5432/).
                     Stored locally (0600); never sent to the server. Each assigned
                     registration derives its own DSN from it + the server-suggested
                     snake_cased name (override per registration via database_names
                     in the host config.json).
  --admin-dsn DSN    Optional provisioning connection (CREATEDB / role bootstrap),
                     used only when creating databases; never stored server-side.
  --dialect D        Dialect served by the base DSN: postgres (default) | mysql | sqlite
                     (sqlite: --dsn is a directory; databases become files in it).
  --skip-preflight   Pair even when the provisioning-rights check fails.

The token comes from the Database Sync page (Sync hosts → Add host) — shown once.
`)
}

// === host pair ===============================================================

func cmdHostPair(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("host pair", flag.ContinueOnError)
	fs.SetOutput(stderr)
	server := fs.String("server", "", "Server base URL")
	dsn := fs.String("dsn", "", "Base server DSN (no database name)")
	adminDSN := fs.String("admin-dsn", "", "Optional provisioning DSN")
	dialect := fs.String("dialect", "postgres", "Dialect: postgres | mysql | sqlite")
	skipPreflight := fs.Bool("skip-preflight", false, "Pair even when the rights check fails")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	serverURL := strings.TrimSpace(*server)
	baseDSN := strings.TrimSpace(*dsn)
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := auth.New(serverURL)
	resp, err := c.HostExchange(ctx, fs.Arg(0))
	if err != nil {
		fmt.Fprintf(stderr, "host pair: failed to validate token: %v\n", err)
		return 1
	}

	// Fail fast while the operator is at the terminal: pairing a host that
	// can't provision is a trap that springs later on someone else's screen.
	report := apply.HostPreflight(ctx, *dialect, baseDSN, strings.TrimSpace(*adminDSN))
	fmt.Fprintf(stdout, "Preflight: %s\n", preflightSummary(report))
	if report.CreateDB != nil && !*report.CreateDB && !*skipPreflight {
		fmt.Fprintln(stderr, "host pair: the provisioning identity lacks the create-database "+
			"right — grant it (postgres: ALTER ROLE ... CREATEDB), pass --admin-dsn, or "+
			"re-run with --skip-preflight if the grant is coming.")
		return 1
	}

	cfg := &config.HostConfig{
		HostID:    resp.HostID,
		HostName:  resp.HostName,
		ServerURL: c.BaseURL,
		Dialect:   *dialect,
	}
	if err := config.SaveHost(cfg, fs.Arg(0), baseDSN, strings.TrimSpace(*adminDSN)); err != nil {
		fmt.Fprintf(stderr, "host pair: save: %v\n", err)
		return 1
	}
	dir, _ := config.HostDir()
	fmt.Fprintf(stdout, "✓ Paired as sync host %q with %s\n", resp.HostName, c.BaseURL)
	fmt.Fprintf(stdout, "  State stored in %s\n", dir)
	fmt.Fprintln(stdout, "  Run 'ee-database host run' to start syncing assigned databases.")
	return 0
}

// === host status / disconnect ===============================================

func cmdHostStatus(stdout, stderr io.Writer) int {
	cfg, refresh, baseDSN, adminDSN, err := config.LoadHost()
	if errors.Is(err, config.ErrHostNotPaired) {
		fmt.Fprintln(stdout, "Not paired as a sync host. Run 'ee-database host pair' first.")
		return 0
	}
	if err != nil {
		fmt.Fprintf(stderr, "host status: %v\n", err)
		return 1
	}
	dir, _ := config.HostDir()
	fmt.Fprintf(stdout, "Sync host %q (%s) @ %s\n", cfg.HostName, cfg.Dialect, cfg.ServerURL)
	fmt.Fprintf(stdout, "  state dir     %s\n", dir)
	fmt.Fprintf(stdout, "  base dsn      saved (%d chars)\n", len(baseDSN))
	fmt.Fprintf(stdout, "  admin dsn     %v\n", adminDSN != "")
	fmt.Fprintf(stdout, "  token len     %d (hidden)\n", len(refresh))
	if len(cfg.DatabaseNames) > 0 {
		fmt.Fprintf(stdout, "  name overrides %v\n", cfg.DatabaseNames)
	}
	return 0
}

func cmdHostDisconnect(stdout, stderr io.Writer) int {
	if err := config.ClearHost(); err != nil {
		fmt.Fprintf(stderr, "host disconnect: %v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, "✓ Host pairing removed locally.")
	fmt.Fprintln(stdout, "  Note: revoke the host on the Database Sync page to fully disable")
	fmt.Fprintln(stdout, "  this machine's access (that also revokes its claimed credentials).")
	return 0
}

// === host run ================================================================

func cmdHostRun(args []string, stdout, stderr io.Writer, logger *log.Logger) int {
	fs := flag.NewFlagSet("host run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	cfg, refresh, baseDSN, adminDSN, err := config.LoadHost()
	if err != nil {
		fmt.Fprintf(stderr, "host run: %v\n", err)
		return 1
	}
	unlock, err := config.LockHost()
	if err != nil {
		fmt.Fprintf(stderr, "host run: %v\n", err)
		return 1
	}
	defer unlock()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Re-check and report, never exit: a host that lost CREATEDB still syncs
	// its already-claimed databases correctly (their ownership is intact);
	// the degraded report drops it from auto-assign eligibility server-side.
	report := apply.HostPreflight(ctx, cfg.Dialect, baseDSN, adminDSN)
	fmt.Fprintf(stdout, "Preflight (host): %s\n", preflightSummary(report))

	runner := &hostRunner{
		ctx:       ctx,
		cfg:       cfg,
		refresh:   refresh,
		baseDSN:   baseDSN,
		adminDSN:  adminDSN,
		preflight: report,
		auth:      auth.New(cfg.ServerURL),
		logger:    logger,
		stdout:    stdout,
		children:  make(map[string]*managedChild),
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

// hostRunner holds the control-plane loop plus one data-plane child per
// claimed database. Children live on the OUTER context, so a control-plane
// reconnect never interrupts running syncs — the reconcile list on the next
// hello trues them up.
type hostRunner struct {
	ctx       context.Context
	cfg       *config.HostConfig
	refresh   string
	baseDSN   string
	adminDSN  string
	preflight framing.PreflightReport
	auth      *auth.Client
	logger    *log.Logger
	stdout    io.Writer

	mu       sync.Mutex
	children map[string]*managedChild
	wg       sync.WaitGroup
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
	conn, err := eesync.DialHost(r.ctx, r.auth.HostWSURL(), access.AccessToken, r.cfg.Dialect)
	if err != nil {
		return false, err
	}
	defer conn.Close(websocket.StatusNormalClosure, "client exit")
	r.logger.Printf("control plane connected ✓ waiting for assignments")

	if err := conn.Send(r.ctx, framing.Preflight{Type: "preflight", Report: r.preflight}); err != nil {
		r.logger.Printf("warning: could not send host preflight: %v", err)
	}

	pingCtx, pingCancel := context.WithCancel(r.ctx)
	defer pingCancel()
	go func() {
		ticker := time.NewTicker(30 * time.Second)
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
		frame, err := conn.RecvHost(r.ctx)
		if err != nil {
			if r.ctx.Err() != nil {
				return true, r.ctx.Err()
			}
			return true, err
		}
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
	r.mu.Lock()
	_, running := r.children[reg.DatabaseID]
	r.mu.Unlock()
	if running {
		return
	}

	cfgP, refreshP, err := config.LoadProfile(reg.DatabaseID)
	if err != nil {
		// Not claimed yet on this machine.
		if !reg.Claimable {
			r.logger.Printf("%s: another client is already paired — skipping "+
				"(revoke its credential or move the sync under this host)", reg.Name)
			return
		}
		claim, err := r.claim(reg.DatabaseID)
		if err != nil {
			var notClaimable auth.ErrNotClaimable
			if errors.As(err, &notClaimable) {
				r.logger.Printf("%s: %v — skipping", reg.Name, err)
			} else {
				r.logger.Printf("%s: claim failed: %v (retried on next reconcile)", reg.Name, err)
			}
			return
		}
		cfgP = &config.Config{
			DatabaseID:   claim.DatabaseID,
			DatabaseName: claim.DatabaseName,
			Dialect:      claim.Dialect,
			ServerURL:    r.cfg.ServerURL,
		}
		if err := config.SaveProfile(reg.DatabaseID, cfgP, claim.RefreshToken); err != nil {
			r.logger.Printf("%s: save profile: %v", reg.Name, err)
			return
		}
		refreshP = claim.RefreshToken
		name := claim.SuggestedDatabaseName
		if override := r.cfg.DatabaseNames[reg.DatabaseID]; override != "" {
			name = override
		}
		dsn, err := apply.DeriveDSN(cfgP.Dialect, r.baseDSN, name)
		if err != nil {
			r.logger.Printf("%s: derive dsn: %v", reg.Name, err)
			return
		}
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

	ensured, err := apply.EnsureDatabase(r.ctx, cfgP.Dialect, dsn, r.adminDSN)
	if err != nil {
		r.logger.Printf("%s: create-missing: %v (retried on next reconcile)", reg.Name, err)
		unlock()
		return
	}
	if ensured.RoleCreated {
		fmt.Fprintf(r.stdout, "✓ Created role for %s\n", reg.Name)
	}
	if ensured.DatabaseCreated {
		fmt.Fprintf(r.stdout, "✓ Created database %s for %s\n", ensured.Name, reg.Name)
	}
	db, err := apply.Open(cfgP.Dialect, dsn)
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

	t := &target{key: reg.DatabaseID, cfg: cfgP, refresh: refreshP, dsn: dsn}
	t.preflight = apply.Preflight(r.ctx, cfgP.Dialect, dsn, db)
	fmt.Fprintf(r.stdout, "Preflight %s: %s\n", targetLabel(t), preflightSummary(t.preflight))

	childCtx, childCancel := context.WithCancel(r.ctx)
	child := &managedChild{cancel: childCancel, done: make(chan struct{})}
	r.mu.Lock()
	r.children[reg.DatabaseID] = child
	r.mu.Unlock()

	childLogger := log.New(os.Stderr, "["+targetLabel(t)+"] ", log.LstdFlags)
	childLogger.Printf("syncing (managed)")
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer close(child.done)
		defer unlock()
		defer db.Close()
		defer func() {
			r.mu.Lock()
			if r.children[reg.DatabaseID] == child {
				delete(r.children, reg.DatabaseID)
			}
			r.mu.Unlock()
		}()
		err := runForever(childCtx, t, db, childLogger)
		if err != nil && !errors.Is(err, context.Canceled) {
			childLogger.Printf("stopped: %v", err)
		} else {
			childLogger.Println("stopped.")
		}
	}()
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

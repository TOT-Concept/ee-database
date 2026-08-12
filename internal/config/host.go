package config

// Managed-host state (issue #65): one host pairing per server, stored beside
// the per-database profiles it factories:
//
//	<root>/host/<server-slug>/config.json   non-secret host metadata + local name overrides
//	<root>/host/<server-slug>/token         host refresh JWT (mode 0600)
//	<root>/host/<server-slug>/dsn           base server DSN, no database name (mode 0600)
//	<root>/host/<server-slug>/admin-dsn     optional provisioning DSN (mode 0600)
//	<root>/host/<server-slug>/lock          flock guard, one managed runner per pairing
//
// The slug is derived from the pairing's normalized server URL, so a machine
// can be a sync host for several backends (dev + prod, staging + prod) at
// once — 'host run' runs every pairing. The pre-1.5.0 layout kept a single
// pairing directly under <root>/host/ and silently overwrote it on re-pair;
// it is migrated into its per-server directory on first access.
//
// The base and admin DSNs never leave this machine — the server knows a host
// exists and what its preflight reported, never how to log into it.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const adminDSNFileName = "admin-dsn"

// HostConfig holds the non-secret managed-host pairing state.
type HostConfig struct {
	HostID    string `json:"host_id"`
	HostName  string `json:"host_name,omitempty"`
	ServerURL string `json:"server_url"`
	Dialect   string `json:"dialect,omitempty"`
	// DatabaseNames overrides the server-suggested physical database name per
	// registration id — for consumers with their own naming rules. Hand-edited;
	// the name is not a credential, so it lives in config.json.
	DatabaseNames map[string]string `json:"database_names,omitempty"`
}

// HostPairing is one server's loaded pairing: config plus its secrets.
type HostPairing struct {
	Slug     string
	Config   *HostConfig
	Refresh  string
	BaseDSN  string
	AdminDSN string
}

// HostSlug derives the pairing directory name from a server URL. The scheme
// is kept ("http-localhost-18808", "https-entityenricher.ai") so two servers
// differing only in scheme cannot collide.
func HostSlug(serverURL string) string {
	s := strings.ToLower(strings.TrimRight(strings.TrimSpace(serverURL), "/"))
	s = strings.Replace(s, "://", "-", 1)
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	slug := strings.Trim(b.String(), "-.")
	if slug == "" {
		return "default"
	}
	return slug
}

func hostRoot() (string, error) {
	d, err := dir()
	if err != nil {
		return "", err
	}
	return filepath.Join(d, "host"), nil
}

// HostPairingDir returns one server's pairing state directory.
func HostPairingDir(serverURL string) (string, error) {
	root, err := hostRoot()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, HostSlug(serverURL)), nil
}

// LockHostPairing guards one server's managed runner (one per pairing).
func LockHostPairing(serverURL string) (func(), error) {
	p, err := HostPairingDir(serverURL)
	if err != nil {
		return nil, err
	}
	return lockDir(p)
}

// migrateLegacyHost moves a pre-1.5.0 single pairing (files directly under
// <root>/host/) into its per-server directory. A legacy 'host run' still
// holds <root>/host/lock while it runs; moving state out from under it would
// let a second runner double-connect, so migration refuses until it stops.
func migrateLegacyHost() error {
	root, err := hostRoot()
	if err != nil {
		return err
	}
	legacyCfgPath := filepath.Join(root, configFileName)
	raw, err := os.ReadFile(legacyCfgPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read legacy host config: %w", err)
	}
	unlock, lockErr := lockDir(root)
	if lockErr != nil {
		return fmt.Errorf("a pre-1.5.0 'ee-database host run' still holds the old host state — stop it, then retry")
	}
	defer unlock()
	var cfg HostConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return fmt.Errorf("parse legacy host config: %w", err)
	}
	dst := filepath.Join(root, HostSlug(cfg.ServerURL))
	if err := os.MkdirAll(dst, dirPerm); err != nil {
		return fmt.Errorf("mkdir %s: %w", dst, err)
	}
	for _, name := range []string{configFileName, tokenFileName, dsnFileName, adminDSNFileName} {
		src := filepath.Join(root, name)
		if _, err := os.Stat(src); errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err := os.Rename(src, filepath.Join(dst, name)); err != nil {
			return fmt.Errorf("migrate host %s: %w", name, err)
		}
	}
	_ = os.Remove(filepath.Join(root, "lock"))
	return nil
}

// SaveHost writes one server's host pairing: config, refresh token and DSNs.
func SaveHost(cfg *HostConfig, refreshToken, baseDSN, adminDSN string) error {
	if err := migrateLegacyHost(); err != nil {
		return err
	}
	p, err := HostPairingDir(cfg.ServerURL)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(p, dirPerm); err != nil {
		return fmt.Errorf("mkdir %s: %w", p, err)
	}
	cfgBytes, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal host config: %w", err)
	}
	if err := os.WriteFile(filepath.Join(p, configFileName), cfgBytes, 0o644); err != nil {
		return fmt.Errorf("write host config: %w", err)
	}
	if err := os.WriteFile(filepath.Join(p, tokenFileName), []byte(refreshToken), secretPerm); err != nil {
		return fmt.Errorf("write host token: %w", err)
	}
	if err := os.WriteFile(filepath.Join(p, dsnFileName), []byte(baseDSN), secretPerm); err != nil {
		return fmt.Errorf("write host dsn: %w", err)
	}
	adminPath := filepath.Join(p, adminDSNFileName)
	if adminDSN == "" {
		_ = os.Remove(adminPath)
		return nil
	}
	return os.WriteFile(adminPath, []byte(adminDSN), secretPerm)
}

// ErrHostNotPaired is returned when no matching host pairing exists.
var ErrHostNotPaired = errors.New(
	"this machine is not paired as a sync host; run 'ee-database host pair --server URL --dsn BASE_DSN <token>' first")

func loadPairingDir(p string) (*HostPairing, error) {
	cfgBytes, err := os.ReadFile(filepath.Join(p, configFileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrHostNotPaired
	}
	if err != nil {
		return nil, fmt.Errorf("read host config: %w", err)
	}
	var cfg HostConfig
	if err := json.Unmarshal(cfgBytes, &cfg); err != nil {
		return nil, fmt.Errorf("parse host config: %w", err)
	}
	token, err := os.ReadFile(filepath.Join(p, tokenFileName))
	if err != nil {
		return nil, ErrHostNotPaired
	}
	baseDSN, err := os.ReadFile(filepath.Join(p, dsnFileName))
	if err != nil {
		return nil, fmt.Errorf("read host dsn: %w", err)
	}
	adminDSN, _ := os.ReadFile(filepath.Join(p, adminDSNFileName)) // optional
	return &HostPairing{
		Slug:     filepath.Base(p),
		Config:   &cfg,
		Refresh:  string(token),
		BaseDSN:  string(baseDSN),
		AdminDSN: string(adminDSN),
	}, nil
}

// LoadHost reads one server's host pairing.
func LoadHost(serverURL string) (*HostPairing, error) {
	if err := migrateLegacyHost(); err != nil {
		return nil, err
	}
	p, err := HostPairingDir(serverURL)
	if err != nil {
		return nil, err
	}
	return loadPairingDir(p)
}

// LoadHosts reads every server's host pairing, sorted by slug.
func LoadHosts() ([]*HostPairing, error) {
	if err := migrateLegacyHost(); err != nil {
		return nil, err
	}
	root, err := hostRoot()
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", root, err)
	}
	var pairings []*HostPairing
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		pairing, err := loadPairingDir(filepath.Join(root, e.Name()))
		if errors.Is(err, ErrHostNotPaired) {
			continue // stray directory without a complete pairing
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", e.Name(), err)
		}
		pairings = append(pairings, pairing)
	}
	sort.Slice(pairings, func(i, j int) bool { return pairings[i].Slug < pairings[j].Slug })
	return pairings, nil
}

// ClearHost removes one server's host pairing. Per-database profiles it
// claimed stay — their credentials are revoked server-side when the host is
// revoked, so the leftovers are inert; 'disconnect' on each removes them.
func ClearHost(serverURL string) error {
	if err := migrateLegacyHost(); err != nil {
		return err
	}
	p, err := HostPairingDir(serverURL)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(p); err != nil {
		return fmt.Errorf("remove %s: %w", p, err)
	}
	return nil
}

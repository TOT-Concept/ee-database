// Package config persists the CLI's pairing state on disk, one profile per
// paired schema database.
//
// Layout (per OS root: macOS/Linux os.UserConfigDir()/ee-database,
// Windows %APPDATA%\ee-database):
//
//	<root>/profiles/<database-id>/config.json
//	<root>/profiles/<database-id>/token                (mode 0600)
//	<root>/profiles/<database-id>/dsn                  (mode 0600, optional --save-dsn)
//	<root>/profiles/<database-id>/lock                 (flock guard, one runner per profile)
//	<root>/profiles/<database-id>/snapshot-failed.sql  (mode 0600, last snapshot that
//	                                                    failed to apply; removed on success)
//
// The pre-0.2.0 single-pairing layout (<root>/{config.json,token,dsn}) is
// migrated into a profile automatically on first access. A profile paired from
// a pasted token before the server reported database metadata lives under the
// key "legacy" until the first run self-heals it (see cli).
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
)

const (
	configFileName         = "config.json"
	tokenFileName          = "token"
	dsnFileName            = "dsn"
	failedSnapshotFileName = "snapshot-failed.sql"
	dirPerm                = 0o700
	secretPerm             = 0o600

	// LegacyKey is the profile key used when the paired database's id is
	// unknown (old server, pre-migration state). Healed on first run.
	LegacyKey = "legacy"
)

// Config holds non-secret pairing state. The refresh token and the DSN live in
// separate 0600 files so they can't leak through casual copies of config.json.
type Config struct {
	DatabaseID   string `json:"database_id"`
	DatabaseName string `json:"database_name,omitempty"`
	Dialect      string `json:"dialect,omitempty"`
	ServerURL    string `json:"server_url"`
	Bootstrapped bool   `json:"bootstrapped,omitempty"`
}

// Profile is one paired database: its directory key plus the loaded config.
type Profile struct {
	Key    string // directory name under profiles/ — the database id once known
	Config *Config
}

// Label returns a human-readable identifier for prompts and logs.
func (p Profile) Label() string {
	if p.Config != nil && p.Config.DatabaseName != "" {
		return p.Config.DatabaseName
	}
	return p.Key
}

func dir() (string, error) {
	if runtime.GOOS == "windows" {
		appData := os.Getenv("APPDATA")
		if appData == "" {
			return "", errors.New("APPDATA environment variable not set")
		}
		return filepath.Join(appData, "ee-database"), nil
	}
	cfgDir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("UserConfigDir: %w", err)
	}
	return filepath.Join(cfgDir, "ee-database"), nil
}

// Path returns the root directory used for ee-database state.
func Path() (string, error) {
	return dir()
}

// ProfileDir returns the state directory of one profile.
func ProfileDir(key string) (string, error) {
	if err := validateKey(key); err != nil {
		return "", err
	}
	d, err := dir()
	if err != nil {
		return "", err
	}
	return filepath.Join(d, "profiles", key), nil
}

// FailedSnapshotPath names the profile-local dump of the last snapshot that
// failed to apply — written by the run loop, removed on the next success.
func FailedSnapshotPath(key string) (string, error) {
	p, err := ProfileDir(key)
	if err != nil {
		return "", err
	}
	return filepath.Join(p, failedSnapshotFileName), nil
}

// validateKey rejects keys that could escape the profiles directory.
func validateKey(key string) error {
	if key == "" || key != filepath.Base(key) || key == "." || key == ".." {
		return fmt.Errorf("invalid profile key %q", key)
	}
	return nil
}

// migrateLegacyLayout moves a pre-profiles pairing (<root>/config.json …)
// into profiles/<database-id or "legacy">/. Idempotent, safe to call always.
func migrateLegacyLayout() error {
	d, err := dir()
	if err != nil {
		return err
	}
	legacyCfg := filepath.Join(d, configFileName)
	raw, err := os.ReadFile(legacyCfg)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read legacy config: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return fmt.Errorf("parse legacy config: %w", err)
	}
	key := cfg.DatabaseID
	if key == "" {
		key = LegacyKey
	}
	target, err := ProfileDir(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(target, dirPerm); err != nil {
		return fmt.Errorf("mkdir %s: %w", target, err)
	}
	for _, name := range []string{configFileName, tokenFileName, dsnFileName} {
		src := filepath.Join(d, name)
		if _, statErr := os.Stat(src); errors.Is(statErr, os.ErrNotExist) {
			continue
		}
		if err := os.Rename(src, filepath.Join(target, name)); err != nil {
			return fmt.Errorf("migrate %s: %w", name, err)
		}
	}
	return nil
}

// ListProfiles returns every paired profile, sorted by label. Runs the legacy
// layout migration first.
func ListProfiles() ([]Profile, error) {
	if err := migrateLegacyLayout(); err != nil {
		return nil, err
	}
	d, err := dir()
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(filepath.Join(d, "profiles"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read profiles dir: %w", err)
	}
	var profiles []Profile
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		cfg, _, loadErr := LoadProfile(e.Name())
		if loadErr != nil {
			continue // half-written or foreign directory; not a pairing
		}
		profiles = append(profiles, Profile{Key: e.Name(), Config: cfg})
	}
	sort.Slice(profiles, func(i, j int) bool {
		return profiles[i].Label() < profiles[j].Label()
	})
	return profiles, nil
}

// SaveProfile writes config.json + token, creating the directory if needed.
func SaveProfile(key string, cfg *Config, refreshToken string) error {
	p, err := ProfileDir(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(p, dirPerm); err != nil {
		return fmt.Errorf("mkdir %s: %w", p, err)
	}
	cfgBytes, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	if err := os.WriteFile(filepath.Join(p, configFileName), cfgBytes, 0o644); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	if err := os.WriteFile(filepath.Join(p, tokenFileName), []byte(refreshToken), secretPerm); err != nil {
		return fmt.Errorf("write token: %w", err)
	}
	return nil
}

// SaveProfileConfig rewrites config.json (e.g. to flip Bootstrapped) without
// touching the token or DSN files.
func SaveProfileConfig(key string, cfg *Config) error {
	p, err := ProfileDir(key)
	if err != nil {
		return err
	}
	cfgBytes, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	return os.WriteFile(filepath.Join(p, configFileName), cfgBytes, 0o644)
}

// SaveProfileDSN stores the connection string in its own 0600 file.
func SaveProfileDSN(key, dsn string) error {
	p, err := ProfileDir(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(p, dirPerm); err != nil {
		return fmt.Errorf("mkdir %s: %w", p, err)
	}
	return os.WriteFile(filepath.Join(p, dsnFileName), []byte(dsn), secretPerm)
}

// LoadProfileDSN returns the profile's stored DSN, or "" when none was saved.
func LoadProfileDSN(key string) string {
	p, err := ProfileDir(key)
	if err != nil {
		return ""
	}
	raw, err := os.ReadFile(filepath.Join(p, dsnFileName))
	if err != nil {
		return ""
	}
	return string(raw)
}

// ErrNotPaired is returned by LoadProfile when the profile does not exist.
var ErrNotPaired = errors.New("ee-database is not paired on this machine; run 'ee-database pair --server URL' first")

// LoadProfile reads one profile's config and refresh token.
func LoadProfile(key string) (*Config, string, error) {
	p, err := ProfileDir(key)
	if err != nil {
		return nil, "", err
	}
	cfgBytes, err := os.ReadFile(filepath.Join(p, configFileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, "", ErrNotPaired
	}
	if err != nil {
		return nil, "", fmt.Errorf("read config: %w", err)
	}
	tokenBytes, err := os.ReadFile(filepath.Join(p, tokenFileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, "", ErrNotPaired
	}
	if err != nil {
		return nil, "", fmt.Errorf("read token: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(cfgBytes, &cfg); err != nil {
		return nil, "", fmt.Errorf("parse config: %w", err)
	}
	return &cfg, string(tokenBytes), nil
}

// RenameProfile moves a profile directory to a new key (self-heal: "legacy" →
// the real database id learned from the exchange response). No-op when the
// target already exists — the newer pairing wins and the stale dir is removed.
func RenameProfile(oldKey, newKey string) error {
	oldDir, err := ProfileDir(oldKey)
	if err != nil {
		return err
	}
	newDir, err := ProfileDir(newKey)
	if err != nil {
		return err
	}
	if _, statErr := os.Stat(newDir); statErr == nil {
		return os.RemoveAll(oldDir)
	}
	return os.Rename(oldDir, newDir)
}

// ClearProfile removes one profile's on-disk pairing state.
func ClearProfile(key string) error {
	p, err := ProfileDir(key)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(p); err != nil {
		return fmt.Errorf("remove %s: %w", p, err)
	}
	return nil
}

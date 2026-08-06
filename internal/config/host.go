package config

// Managed-host state (issue #65): one host pairing per machine, stored beside
// the per-database profiles it factories:
//
//	<root>/host/config.json   non-secret host metadata + local name overrides
//	<root>/host/token         host refresh JWT (mode 0600)
//	<root>/host/dsn           base server DSN, no database name (mode 0600)
//	<root>/host/admin-dsn     optional provisioning DSN (mode 0600)
//	<root>/host/lock          flock guard, one managed runner per machine
//
// The base and admin DSNs never leave this machine — the server knows a host
// exists and what its preflight reported, never how to log into it.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

// HostDir returns the managed-host state directory.
func HostDir() (string, error) {
	d, err := dir()
	if err != nil {
		return "", err
	}
	return filepath.Join(d, "host"), nil
}

// LockHost guards the managed runner (one per machine).
func LockHost() (func(), error) {
	p, err := HostDir()
	if err != nil {
		return nil, err
	}
	return lockDir(p)
}

// SaveHost writes the host pairing: config, refresh token and DSNs.
func SaveHost(cfg *HostConfig, refreshToken, baseDSN, adminDSN string) error {
	p, err := HostDir()
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

// ErrHostNotPaired is returned by LoadHost when no host pairing exists.
var ErrHostNotPaired = errors.New(
	"this machine is not paired as a sync host; run 'ee-database host pair --server URL --dsn BASE_DSN <token>' first")

// LoadHost reads the host pairing: config, refresh token, base and admin DSNs.
func LoadHost() (*HostConfig, string, string, string, error) {
	p, err := HostDir()
	if err != nil {
		return nil, "", "", "", err
	}
	cfgBytes, err := os.ReadFile(filepath.Join(p, configFileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, "", "", "", ErrHostNotPaired
	}
	if err != nil {
		return nil, "", "", "", fmt.Errorf("read host config: %w", err)
	}
	var cfg HostConfig
	if err := json.Unmarshal(cfgBytes, &cfg); err != nil {
		return nil, "", "", "", fmt.Errorf("parse host config: %w", err)
	}
	token, err := os.ReadFile(filepath.Join(p, tokenFileName))
	if err != nil {
		return nil, "", "", "", ErrHostNotPaired
	}
	baseDSN, err := os.ReadFile(filepath.Join(p, dsnFileName))
	if err != nil {
		return nil, "", "", "", fmt.Errorf("read host dsn: %w", err)
	}
	adminDSN, _ := os.ReadFile(filepath.Join(p, adminDSNFileName)) // optional
	return &cfg, string(token), string(baseDSN), string(adminDSN), nil
}

// ClearHost removes the host pairing. Per-database profiles it claimed stay —
// their credentials are revoked server-side when the host is revoked, so the
// leftovers are inert; 'disconnect' on each removes them.
func ClearHost() error {
	p, err := HostDir()
	if err != nil {
		return err
	}
	if err := os.RemoveAll(p); err != nil {
		return fmt.Errorf("remove %s: %w", p, err)
	}
	return nil
}

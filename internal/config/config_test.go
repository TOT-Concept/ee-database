package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// redirect the state root into a temp dir (UserConfigDir derives from these).
func tempRoot(t *testing.T) string {
	t.Helper()
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(tmp, ".config"))
	t.Setenv("APPDATA", tmp)
	d, err := dir()
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestMigrateLegacyLayout(t *testing.T) {
	root := tempRoot(t)
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	legacy := Config{DatabaseID: "abc-123", DatabaseName: "prod", ServerURL: "https://x"}
	raw, _ := json.Marshal(legacy)
	must(t, os.WriteFile(filepath.Join(root, "config.json"), raw, 0o644))
	must(t, os.WriteFile(filepath.Join(root, "token"), []byte("tok"), 0o600))
	must(t, os.WriteFile(filepath.Join(root, "dsn"), []byte("postgres://x"), 0o600))

	profiles, err := ListProfiles()
	if err != nil {
		t.Fatal(err)
	}
	if len(profiles) != 1 || profiles[0].Key != "abc-123" {
		t.Fatalf("expected one profile keyed abc-123, got %+v", profiles)
	}
	cfg, token, err := LoadProfile("abc-123")
	if err != nil || cfg.DatabaseName != "prod" || token != "tok" {
		t.Fatalf("migrated profile broken: %+v %q %v", cfg, token, err)
	}
	if LoadProfileDSN("abc-123") != "postgres://x" {
		t.Fatal("dsn not migrated")
	}
	if _, err := os.Stat(filepath.Join(root, "config.json")); !os.IsNotExist(err) {
		t.Fatal("legacy config.json should be gone")
	}
}

func TestMigrateLegacyLayoutWithoutDatabaseID(t *testing.T) {
	root := tempRoot(t)
	must(t, os.MkdirAll(root, 0o700))
	raw, _ := json.Marshal(Config{ServerURL: "https://x"})
	must(t, os.WriteFile(filepath.Join(root, "config.json"), raw, 0o644))
	must(t, os.WriteFile(filepath.Join(root, "token"), []byte("tok"), 0o600))

	profiles, err := ListProfiles()
	if err != nil {
		t.Fatal(err)
	}
	if len(profiles) != 1 || profiles[0].Key != LegacyKey {
		t.Fatalf("expected the legacy key, got %+v", profiles)
	}
}

func TestSaveLoadRenameClear(t *testing.T) {
	tempRoot(t)
	cfg := &Config{DatabaseID: "id-1", DatabaseName: "analytics", ServerURL: "https://x"}
	if err := SaveProfile("legacy", cfg, "tok"); err != nil {
		t.Fatal(err)
	}
	if err := SaveProfileDSN("legacy", "postgres://y"); err != nil {
		t.Fatal(err)
	}
	if err := RenameProfile("legacy", "id-1"); err != nil {
		t.Fatal(err)
	}
	if LoadProfileDSN("id-1") != "postgres://y" {
		t.Fatal("dsn lost in rename")
	}
	if _, _, err := LoadProfile("legacy"); err != ErrNotPaired {
		t.Fatalf("legacy profile should be gone, got %v", err)
	}
	if err := ClearProfile("id-1"); err != nil {
		t.Fatal(err)
	}
	profiles, _ := ListProfiles()
	if len(profiles) != 0 {
		t.Fatalf("expected no profiles, got %+v", profiles)
	}
}

func TestRenameOntoExistingKeepsTarget(t *testing.T) {
	tempRoot(t)
	must(t, SaveProfile("legacy", &Config{ServerURL: "https://old"}, "old-tok"))
	must(t, SaveProfile("id-2", &Config{DatabaseID: "id-2", ServerURL: "https://new"}, "new-tok"))
	if err := RenameProfile("legacy", "id-2"); err != nil {
		t.Fatal(err)
	}
	_, token, err := LoadProfile("id-2")
	if err != nil || token != "new-tok" {
		t.Fatalf("newer pairing should win: %q %v", token, err)
	}
	if _, _, err := LoadProfile("legacy"); err != ErrNotPaired {
		t.Fatal("stale legacy profile should be removed")
	}
}

func TestValidateKeyRejectsTraversal(t *testing.T) {
	tempRoot(t)
	for _, bad := range []string{"", "..", "a/b", "../x"} {
		if _, err := ProfileDir(bad); err == nil {
			t.Fatalf("key %q should be rejected", bad)
		}
	}
}

func TestLockProfileExcludesSecondHolder(t *testing.T) {
	tempRoot(t)
	unlock, err := LockProfile("id-3")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LockProfile("id-3"); err == nil {
		t.Fatal("second lock should fail while the first is held")
	}
	unlock()
	unlock2, err := LockProfile("id-3")
	if err != nil {
		t.Fatalf("lock should succeed after unlock: %v", err)
	}
	unlock2()
}

// The generation of the target database is part of a pairing; a profile made
// before the server counted it reads as generation 0, and writes no field.
func TestTargetGenerationRoundTrip(t *testing.T) {
	root := tempRoot(t)
	must(t, SaveProfile("id-4", &Config{DatabaseID: "id-4", ServerURL: "https://x", TargetGeneration: 2}, "tok"))
	cfg, _, err := LoadProfile("id-4")
	if err != nil || cfg.TargetGeneration != 2 {
		t.Fatalf("generation not kept: %+v %v", cfg, err)
	}
	must(t, SaveProfile("id-5", &Config{DatabaseID: "id-5", ServerURL: "https://x"}, "tok"))
	raw, err := os.ReadFile(filepath.Join(root, "profiles", "id-5", "config.json"))
	must(t, err)
	var fields map[string]any
	must(t, json.Unmarshal(raw, &fields))
	if _, written := fields["target_generation"]; written {
		t.Fatalf("generation 0 should not be written: %s", raw)
	}
}

func TestClearProfileDSNKeepsThePairing(t *testing.T) {
	tempRoot(t)
	must(t, SaveProfile("id-6", &Config{DatabaseID: "id-6", ServerURL: "https://x", Bootstrapped: true}, "tok"))
	must(t, SaveProfileDSN("id-6", "postgres://y"))
	must(t, ClearProfileDSN("id-6"))
	if dsn := LoadProfileDSN("id-6"); dsn != "" {
		t.Fatalf("dsn still saved: %q", dsn)
	}
	cfg, token, err := LoadProfile("id-6")
	if err != nil || token != "tok" || !cfg.Bootstrapped {
		t.Fatalf("the pairing itself must survive: %+v %q %v", cfg, token, err)
	}
	// Nothing saved is nothing to forget.
	must(t, ClearProfileDSN("id-6"))
	if err := ClearProfileDSN("../x"); err == nil {
		t.Fatal("an invalid key must be refused")
	}
}

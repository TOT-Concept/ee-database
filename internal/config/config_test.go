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

func TestMigrateLegacyLayout(t *testing.T) {
	root := tempRoot(t)
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	legacy := Config{DatabaseID: "abc-123", DatabaseName: "prod", ServerURL: "https://x"}
	raw, _ := json.Marshal(legacy)
	os.WriteFile(filepath.Join(root, "config.json"), raw, 0o644)
	os.WriteFile(filepath.Join(root, "token"), []byte("tok"), 0o600)
	os.WriteFile(filepath.Join(root, "dsn"), []byte("postgres://x"), 0o600)

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
	os.MkdirAll(root, 0o700)
	raw, _ := json.Marshal(Config{ServerURL: "https://x"})
	os.WriteFile(filepath.Join(root, "config.json"), raw, 0o644)
	os.WriteFile(filepath.Join(root, "token"), []byte("tok"), 0o600)

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
	SaveProfile("legacy", &Config{ServerURL: "https://old"}, "old-tok")
	SaveProfile("id-2", &Config{DatabaseID: "id-2", ServerURL: "https://new"}, "new-tok")
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

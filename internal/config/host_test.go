package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestHostSlug(t *testing.T) {
	cases := map[string]string{
		"http://localhost:18808":     "http-localhost-18808",
		"http://localhost:18908":     "http-localhost-18908",
		"https://entityenricher.ai":  "https-entityenricher.ai",
		"https://EntityEnricher.ai/": "https-entityenricher.ai",
		"":                           "default",
	}
	for in, want := range cases {
		if got := HostSlug(in); got != want {
			t.Errorf("HostSlug(%q) = %q, want %q", in, got, want)
		}
	}
	if HostSlug("http://x") == HostSlug("https://x") {
		t.Error("slugs must not collide across schemes")
	}
}

func TestSaveLoadClearMultipleHosts(t *testing.T) {
	tempRoot(t)
	a := &HostConfig{HostID: "h1", HostName: "dev", ServerURL: "http://localhost:18808"}
	b := &HostConfig{HostID: "h2", HostName: "test", ServerURL: "http://localhost:18908"}
	must(t, SaveHost(a, "tok-a", "postgres://a/", "postgres://admin-a/"))
	must(t, SaveHost(b, "tok-b", "postgres://b/", ""))

	pairings, err := LoadHosts()
	if err != nil || len(pairings) != 2 {
		t.Fatalf("expected 2 pairings, got %d (%v)", len(pairings), err)
	}
	got, err := LoadHost("http://localhost:18908")
	if err != nil || got.Config.HostID != "h2" || got.Refresh != "tok-b" || got.BaseDSN != "postgres://b/" {
		t.Fatalf("pairing b broken: %+v %v", got, err)
	}
	if got.AdminDSN != "" {
		t.Fatalf("pairing b should have no admin dsn, got %q", got.AdminDSN)
	}
	got, err = LoadHost("http://localhost:18808")
	if err != nil || got.AdminDSN != "postgres://admin-a/" {
		t.Fatalf("pairing a admin dsn broken: %+v %v", got, err)
	}

	must(t, ClearHost("http://localhost:18808"))
	pairings, _ = LoadHosts()
	if len(pairings) != 1 || pairings[0].Config.HostID != "h2" {
		t.Fatalf("expected only pairing b left, got %+v", pairings)
	}
	if _, err := LoadHost("http://localhost:18808"); err != ErrHostNotPaired {
		t.Fatalf("cleared pairing should be gone, got %v", err)
	}
}

func TestRepairSameServerOverwrites(t *testing.T) {
	tempRoot(t)
	must(t, SaveHost(&HostConfig{HostID: "old", ServerURL: "http://localhost:18808"},
		"tok-old", "postgres://a/", "postgres://admin/"))
	must(t, SaveHost(&HostConfig{HostID: "new", ServerURL: "http://localhost:18808"},
		"tok-new", "postgres://a/", ""))
	pairings, _ := LoadHosts()
	if len(pairings) != 1 || pairings[0].Config.HostID != "new" || pairings[0].Refresh != "tok-new" {
		t.Fatalf("re-pair should replace in place, got %+v", pairings)
	}
	if pairings[0].AdminDSN != "" {
		t.Fatal("re-pair without admin dsn should remove the stored one")
	}
}

func TestMigrateLegacyHostLayout(t *testing.T) {
	root := tempRoot(t)
	legacy := filepath.Join(root, "host")
	must(t, os.MkdirAll(legacy, 0o700))
	raw, _ := json.Marshal(HostConfig{HostID: "h1", HostName: "old", ServerURL: "http://localhost:18908"})
	must(t, os.WriteFile(filepath.Join(legacy, configFileName), raw, 0o644))
	must(t, os.WriteFile(filepath.Join(legacy, tokenFileName), []byte("tok"), 0o600))
	must(t, os.WriteFile(filepath.Join(legacy, dsnFileName), []byte("postgres://x/"), 0o600))
	must(t, os.WriteFile(filepath.Join(legacy, adminDSNFileName), []byte("postgres://adm/"), 0o600))

	pairings, err := LoadHosts()
	if err != nil || len(pairings) != 1 {
		t.Fatalf("expected the migrated pairing, got %+v (%v)", pairings, err)
	}
	p := pairings[0]
	if p.Slug != "http-localhost-18908" || p.Config.HostID != "h1" ||
		p.Refresh != "tok" || p.BaseDSN != "postgres://x/" || p.AdminDSN != "postgres://adm/" {
		t.Fatalf("migration lost state: %+v", p)
	}
	if _, err := os.Stat(filepath.Join(legacy, configFileName)); !os.IsNotExist(err) {
		t.Fatal("legacy host config.json should be gone")
	}
}

func TestMigrateLegacyHostRefusedWhileLegacyRunnerHoldsLock(t *testing.T) {
	root := tempRoot(t)
	legacy := filepath.Join(root, "host")
	must(t, os.MkdirAll(legacy, 0o700))
	raw, _ := json.Marshal(HostConfig{ServerURL: "http://localhost:18908"})
	must(t, os.WriteFile(filepath.Join(legacy, configFileName), raw, 0o644))
	must(t, os.WriteFile(filepath.Join(legacy, tokenFileName), []byte("tok"), 0o600))
	unlock, err := lockDir(legacy)
	must(t, err)
	defer unlock()

	if _, err := LoadHosts(); err == nil {
		t.Fatal("migration should refuse while the legacy runner holds the lock")
	}
}

func TestLockHostPairingExcludesSecondHolder(t *testing.T) {
	tempRoot(t)
	unlock, err := LockHostPairing("http://localhost:18808")
	must(t, err)
	if _, err := LockHostPairing("http://localhost:18808"); err == nil {
		t.Fatal("second lock on the same pairing should fail")
	}
	unlock2, err := LockHostPairing("http://localhost:18908")
	if err != nil {
		t.Fatalf("a different server's pairing lock must be independent: %v", err)
	}
	unlock()
	unlock2()
}

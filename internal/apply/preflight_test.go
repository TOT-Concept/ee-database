package apply

import (
	"context"
	"testing"
)

// The sqlite driver is pure Go, so the probe runs for real against :memory:.
func TestPreflightSQLite(t *testing.T) {
	db, err := Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	report := Preflight(context.Background(), "sqlite", ":memory:", db)
	if report.Dialect != "sqlite" {
		t.Errorf("dialect = %q", report.Dialect)
	}
	for name, got := range map[string]*bool{
		"create_db": report.CreateDB, "ddl": report.DDL, "dml": report.DML,
	} {
		if got == nil || !*got {
			t.Errorf("%s = %v, want true", name, got)
		}
	}
	// The probe must clean up after itself.
	var count int
	err = db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?", probeTable,
	).Scan(&count)
	if err != nil || count != 0 {
		t.Errorf("probe table left behind (count=%d, err=%v)", count, err)
	}
}

func TestPreflightUnsupportedDialect(t *testing.T) {
	report := Preflight(context.Background(), "oracle", "whatever", nil)
	if report.CreateDB != nil || report.DDL != nil || report.DML != nil {
		t.Errorf("unsupported dialect must determine nothing: %+v", report)
	}
	if len(report.Details) == 0 {
		t.Error("unsupported dialect should carry a detail")
	}
}

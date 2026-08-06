package apply

import (
	"strings"
	"testing"
)

func TestDeriveDSNPostgres(t *testing.T) {
	got, err := DeriveDSN("postgres", "postgres://ee_replica:pw@localhost:15438/", "vinyl_db")
	if err != nil || got != "postgres://ee_replica:pw@localhost:15438/vinyl_db" {
		t.Errorf("got %q err=%v", got, err)
	}
	// Query params (sslmode) must survive the path swap.
	got, err = DeriveDSN("", "postgres://u:p@h:5432/?sslmode=require", "my_db")
	if err != nil || got != "postgres://u:p@h:5432/my_db?sslmode=require" {
		t.Errorf("with params: got %q err=%v", got, err)
	}
}

func TestDeriveDSNMySQL(t *testing.T) {
	got, err := DeriveDSN("mysql", "user:pw@tcp(localhost:3306)/", "vinyl_db")
	if err != nil || !strings.Contains(got, "/vinyl_db") {
		t.Errorf("got %q err=%v", got, err)
	}
}

func TestDeriveDSNSQLite(t *testing.T) {
	got, err := DeriveDSN("sqlite", "/var/lib/ee", "vinyl_db")
	if err != nil || got != "/var/lib/ee/vinyl_db.db" {
		t.Errorf("got %q err=%v", got, err)
	}
}

func TestDeriveDSNRejects(t *testing.T) {
	if _, err := DeriveDSN("postgres", "not a url", "db"); err == nil {
		t.Error("want error for non-URL postgres base dsn")
	}
	if _, err := DeriveDSN("oracle", "x", "db"); err == nil {
		t.Error("want error for unsupported dialect")
	}
	if _, err := DeriveDSN("postgres", "postgres://u@h/", ""); err == nil {
		t.Error("want error for empty database name")
	}
}

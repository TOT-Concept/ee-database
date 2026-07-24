package apply

import (
	"context"
	"strings"
	"testing"
)

func TestQuoteIdent(t *testing.T) {
	if got := pgQuoteIdent(`video_games`); got != `"video_games"` {
		t.Errorf("pgQuoteIdent = %s", got)
	}
	if got := pgQuoteIdent(`we"ird`); got != `"we""ird"` {
		t.Errorf("pgQuoteIdent escaping = %s", got)
	}
	if got := mysqlQuoteIdent("video_games"); got != "`video_games`" {
		t.Errorf("mysqlQuoteIdent = %s", got)
	}
	if got := mysqlQuoteIdent("we`ird"); got != "`we``ird`" {
		t.Errorf("mysqlQuoteIdent escaping = %s", got)
	}
}

func TestQuoteLiterals(t *testing.T) {
	if got := pgQuoteLiteral("pa'ss"); got != "'pa''ss'" {
		t.Errorf("pgQuoteLiteral = %s", got)
	}
	if got := mysqlQuoteString(`pa's\s`); got != `'pa''s\\s'` {
		t.Errorf("mysqlQuoteString = %s", got)
	}
	if got := mysqlQuoteAccount("ee_replica"); got != "'ee_replica'@'%'" {
		t.Errorf("mysqlQuoteAccount = %s", got)
	}
}

func TestEnsureDatabaseSQLiteIsNoOp(t *testing.T) {
	res, err := EnsureDatabase(context.Background(), "sqlite", "file:whatever.db", "")
	if err != nil || res.DatabaseCreated || res.RoleCreated {
		t.Errorf("sqlite ensure: %+v err=%v, want no-op", res, err)
	}
}

func TestEnsureDatabaseUnsupportedDialect(t *testing.T) {
	_, err := EnsureDatabase(context.Background(), "oracle", "whatever", "")
	if err == nil || !strings.Contains(err.Error(), "unsupported dialect") {
		t.Errorf("err = %v, want unsupported dialect", err)
	}
}

func TestEnsureDatabaseMySQLRequiresName(t *testing.T) {
	_, err := EnsureDatabase(context.Background(), "mysql", "user:pass@tcp(localhost:3306)/", "")
	if err == nil || !strings.Contains(err.Error(), "names no database") {
		t.Errorf("err = %v, want missing-name error", err)
	}
}

func TestEnsureDatabaseBadPostgresDSN(t *testing.T) {
	_, err := EnsureDatabase(context.Background(), "postgres", "postgres://\x00bad", "")
	if err == nil {
		t.Error("want parse error for invalid postgres dsn")
	}
}

func TestEnsureDatabaseBadAdminDSN(t *testing.T) {
	_, err := EnsureDatabase(
		context.Background(), "postgres",
		"postgres://ee_replica:pw@localhost:5432/replica_db", "postgres://\x00bad",
	)
	if err == nil || !strings.Contains(err.Error(), "admin dsn") {
		t.Errorf("err = %v, want admin dsn parse error", err)
	}
}

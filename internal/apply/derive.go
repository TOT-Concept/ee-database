package apply

// DeriveDSN composes a per-database DSN from the host's base DSN (a server,
// no database name) and the physical database name. The base DSN never leaves
// the machine; the name is the server-suggested snake_cased registration name
// unless overridden in the host config.

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	gomysql "github.com/go-sql-driver/mysql"
)

func DeriveDSN(dialect, baseDSN, databaseName string) (string, error) {
	if databaseName == "" {
		return "", fmt.Errorf("empty database name")
	}
	switch dialect {
	case "", "postgres":
		u, err := url.Parse(strings.TrimSpace(baseDSN))
		if err != nil {
			return "", fmt.Errorf("postgres base dsn: %w", err)
		}
		if u.Scheme == "" {
			return "", fmt.Errorf("postgres base dsn must be a URL (postgres://user:pass@host:port/)")
		}
		u.Path = "/" + databaseName
		return u.String(), nil
	case "mysql":
		cfg, err := gomysql.ParseDSN(strings.TrimSpace(baseDSN))
		if err != nil {
			return "", fmt.Errorf("mysql base dsn: %w", err)
		}
		cfg.DBName = databaseName
		return cfg.FormatDSN(), nil
	case "sqlite":
		// The base "DSN" is a directory; each database becomes a file in it.
		return filepath.Join(strings.TrimSpace(baseDSN), databaseName+".db"), nil
	default:
		return "", fmt.Errorf("unsupported dialect %q", dialect)
	}
}

package apply

import (
	"context"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"
)

// TestDiagnoseConnectLive breaks one parameter at a time of the live test
// DSNs (EE_TEST_POSTGRES_DSN, EE_TEST_SQLSERVER_DSN) and checks that the
// failure points at that parameter, which is the one `run` asks for again.
func TestDiagnoseConnectLive(t *testing.T) {
	for _, tc := range []struct{ env, dialect string }{
		{"EE_TEST_POSTGRES_DSN", "postgres"},
		{"EE_TEST_SQLSERVER_DSN", "sqlserver"},
	} {
		t.Run(tc.dialect, func(t *testing.T) {
			raw := os.Getenv(tc.env)
			if raw == "" {
				t.Skip(tc.env + " not set")
			}
			driver, _ := For(tc.dialect)
			u, err := url.Parse(raw)
			if err != nil {
				t.Fatal(err)
			}
			password, _ := u.User.Password()
			port, _ := strconv.Atoi(u.Port())
			good := DSNParts{Host: u.Hostname(), Port: port, User: u.User.Username(),
				Password: password, Database: map[string]string{"postgres": "postgres", "sqlserver": "master"}[tc.dialect]}

			for _, c := range []struct {
				name  string
				edit  func(*DSNParts)
				wants ConnectProblem
			}{
				{"reachable", func(*DSNParts) {}, -1},
				{"wrong port", func(p *DSNParts) { p.Port = 1 }, ProblemEndpoint},
				{"unknown host", func(p *DSNParts) { p.Host = "ee-no-such-host.invalid" }, ProblemEndpoint},
				{"wrong password", func(p *DSNParts) { p.Password += "x" }, ProblemLogin},
				{"unknown user", func(p *DSNParts) { p.User = "ee_no_such_user" }, ProblemLogin},
				{"unknown database", func(p *DSNParts) { p.Database = "ee_no_such_database" }, ProblemDatabase},
			} {
				t.Run(c.name, func(t *testing.T) {
					parts := good
					c.edit(&parts)
					db, err := driver.Open(driver.ComposeDSN(parts))
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					err = db.PingContext(ctx)
					if c.wants < 0 {
						if err != nil {
							t.Fatalf("good parameters do not connect: %v", err)
						}
						return
					}
					if err == nil {
						t.Fatal("connected")
					}
					got, why := DiagnoseConnect(driver, err)
					t.Logf("%s", why)
					if got != c.wants {
						t.Errorf("problem = %d, want %d: %v", got, c.wants, err)
					}
				})
			}
		})
	}
}

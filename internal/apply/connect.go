package apply

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"syscall"
)

// ConnectProblem is the connection parameter a failed connection to a target
// points at, so the operator is asked again for that parameter only.
type ConnectProblem int

const (
	// ProblemUnknown: the failure names no parameter.
	ProblemUnknown ConnectProblem = iota
	// ProblemEndpoint: nothing answers at the host and port.
	ProblemEndpoint
	// ProblemLogin: the server refused the user or the password. Neither
	// PostgreSQL nor SQL Server tells the client which of the two.
	ProblemLogin
	// ProblemDatabase: the database does not exist, or the user may not open it.
	ProblemDatabase
)

// DiagnoseConnect reads a failed open or ping of a target: the parameter it
// points at, and what to check, in words, followed by the driver's own error.
func DiagnoseConnect(d Driver, err error) (ConnectProblem, string) {
	var dnsErr *net.DNSError
	var why string
	problem := d.connectProblem(err)
	switch {
	case problem == ProblemLogin:
		why = d.Label() + " refused the user or the password"
	case problem == ProblemDatabase:
		why = "the database does not exist, or this user may not open it " +
			"(--create-missing creates it)"
	case errors.As(err, &dnsErr) || mentions(err, "no such host"):
		problem, why = ProblemEndpoint, "host not found: check the host name"
	case errors.Is(err, syscall.ECONNREFUSED) || mentions(err, "connection refused"):
		problem, why = ProblemEndpoint, "nothing answers at that host and port: is "+d.Label()+" running there?"
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) ||
		mentions(err, "i/o timeout"):
		problem, why = ProblemEndpoint, "no answer in time: check the host, the port and any firewall"
	default:
		return ProblemUnknown, err.Error()
	}
	return problem, why + "\n  (" + lastLine(err.Error()) + ")"
}

// lastLine keeps the verdict of an error that lists every address and TLS
// mode the driver tried (pgx), one per line, the deciding one last.
func lastLine(text string) string {
	lines := strings.Split(strings.TrimSpace(text), "\n")
	return strings.TrimSpace(lines[len(lines)-1])
}

// mentions covers drivers that format a network error into their own text
// instead of wrapping it.
func mentions(err error, text string) bool {
	return strings.Contains(err.Error(), text)
}

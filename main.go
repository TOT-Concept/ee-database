// ee-database — self-hosted apply client for Entity Enricher schema databases.
//
// Pairs with a schema database, receives delta batches over an outbound
// WebSocket, applies them to your own database (PostgreSQL / MySQL / SQLite)
// and acknowledges. Your DSN never leaves this machine.
package main

import (
	"os"

	"github.com/TOT-Concept/ee-database/internal/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:], os.Stdout, os.Stderr))
}

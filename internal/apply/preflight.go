package apply

import (
	"context"
	"database/sql"
	"fmt"
)

// probeTable is created, written and dropped by the DDL/DML probe. A real
// table (not TEMP) because TEMP proves a different privilege than the deltas
// need, and a probe is the one check that reads the same on every target.
const probeTable = "_ee_preflight"

// provisioningDSN picks the connection a host creates databases through, and
// what to call it in a message: the admin DSN when given (said in details, so
// the report names the identity it checked), else the base DSN's own login.
func provisioningDSN(baseDSN, adminDSN string, details *[]string) (dsn, label string) {
	if adminDSN == "" {
		return baseDSN, "base dsn"
	}
	*details = append(*details, "database creation uses --admin-dsn")
	return adminDSN, "admin dsn"
}

// probeDDLDML proves the DDL and DML rights by using them. createProbe is the
// driver's create-when-missing statement for probeTable (the one statement of
// the probe that has no portable form).
func probeDDLDML(ctx context.Context, db *sql.DB, createProbe string, details *[]string) (ddl, dml *bool) {
	no, yes := false, true
	if _, err := db.ExecContext(ctx, createProbe); err != nil {
		*details = append(*details, fmt.Sprintf("DDL probe (CREATE TABLE) failed: %v", err))
		return &no, nil // cannot tell DML apart without a table to write
	}
	ddl = &yes

	dml = &yes
	if _, err := db.ExecContext(ctx,
		"INSERT INTO "+probeTable+" (id) VALUES (1)"); err != nil {
		*details = append(*details, fmt.Sprintf("DML probe (INSERT) failed: %v", err))
		dml = &no
	} else if _, err := db.ExecContext(ctx, "DELETE FROM "+probeTable); err != nil {
		*details = append(*details, fmt.Sprintf("DML probe (DELETE) failed: %v", err))
		dml = &no
	}

	if _, err := db.ExecContext(ctx, "DROP TABLE "+probeTable); err != nil {
		*details = append(*details, fmt.Sprintf("probe cleanup (DROP TABLE) failed: %v", err))
	}
	return ddl, dml
}

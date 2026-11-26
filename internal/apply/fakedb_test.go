package apply

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"
)

// fakeDB is a database/sql driver that runs nothing: it records what the code
// under test sends — every Exec, and BEGIN / COMMIT / ROLLBACK / CLOSE as
// words — and refuses any statement containing failOn. It proves what this
// package owns (statement order, transaction boundaries, what a failure
// reports) without a database; the SQL itself is proven by the live tests.
type fakeDB struct {
	failOn string

	mu  sync.Mutex
	log []string
}

func openFake(t *testing.T) (*sql.DB, *fakeDB) {
	t.Helper()
	fake := &fakeDB{}
	db := sql.OpenDB(fake)
	t.Cleanup(func() { _ = db.Close() })
	return db, fake
}

func (f *fakeDB) record(entry string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log = append(f.log, entry)
}

// sent returns what was recorded so far and starts a fresh record.
func (f *fakeDB) sent() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.log
	f.log = nil
	return out
}

func (f *fakeDB) Connect(context.Context) (driver.Conn, error) { return fakeConn{f}, nil }
func (f *fakeDB) Driver() driver.Driver                        { return f }
func (f *fakeDB) Open(string) (driver.Conn, error)             { return fakeConn{f}, nil }

type fakeConn struct{ db *fakeDB }

func (c fakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("fakedb: prepared statements are not supported")
}

func (c fakeConn) Close() error {
	c.db.record("CLOSE")
	return nil
}

func (c fakeConn) Begin() (driver.Tx, error) {
	c.db.record("BEGIN")
	return fakeTx(c), nil
}

func (c fakeConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.db.record(query)
	if c.db.failOn != "" && strings.Contains(query, c.db.failOn) {
		return nil, errors.New("fakedb: statement refused")
	}
	return driver.RowsAffected(0), nil
}

type fakeTx struct{ db *fakeDB }

func (t fakeTx) Commit() error {
	t.db.record("COMMIT")
	return nil
}

func (t fakeTx) Rollback() error {
	t.db.record("ROLLBACK")
	return nil
}

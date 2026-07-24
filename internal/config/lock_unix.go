//go:build !windows

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// LockProfile takes an exclusive advisory lock on the profile's lock file so
// two ee-database processes can't run the same pairing concurrently (they
// would evict each other's WebSocket session forever). Returns an unlock
// function; errors mention the holder scenario explicitly.
func LockProfile(key string) (func(), error) {
	p, err := ProfileDir(key)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(p, dirPerm); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", p, err)
	}
	f, err := os.OpenFile(filepath.Join(p, "lock"), os.O_CREATE|os.O_RDWR, secretPerm)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf(
			"another ee-database process is already running this database's sync")
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, nil
}

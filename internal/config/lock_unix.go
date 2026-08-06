//go:build !windows

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// lockDir takes an exclusive advisory lock on dir/lock so two ee-database
// processes can't run the same pairing (or the same host) concurrently — they
// would evict each other's WebSocket session forever. Returns an unlock
// function; errors mention the holder scenario explicitly.
func lockDir(dir string) (func(), error) {
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR, secretPerm)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf(
			"another ee-database process is already running this sync")
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, nil
}

// LockProfile locks one paired database's profile directory.
func LockProfile(key string) (func(), error) {
	p, err := ProfileDir(key)
	if err != nil {
		return nil, err
	}
	return lockDir(p)
}

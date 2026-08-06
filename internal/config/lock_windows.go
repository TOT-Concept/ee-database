//go:build windows

package config

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

// lockDir takes an exclusive lock on dir/lock so two ee-database processes
// can't run the same pairing (or the same host) concurrently — they would
// evict each other's WebSocket session forever. Returns an unlock function.
func lockDir(dir string) (func(), error) {
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR, secretPerm)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	ol := new(windows.Overlapped)
	err = windows.LockFileEx(windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0, 1, 0, ol)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf(
			"another ee-database process is already running this sync")
	}
	return func() {
		_ = windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, ol)
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

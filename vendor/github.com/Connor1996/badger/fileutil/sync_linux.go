// +build linux

package fileutil

import (
	"os"

	"golang.org/x/sys/unix"
)

// Fsync is a wrapper around file.Sync(). Special handling is needed on darwin platform.
func Fsync(f *os.File) error {
	return f.Sync()
}

// Fdatasync is similar to fsync(), but does not flush modified metadata
// unless that metadata is needed in order to allow a subsequent data retrieval
// to be correctly handled.
func Fdatasync(f *os.File) error {
	return unix.Fdatasync(int(f.Fd()))
}

// +build !linux,!darwin

package fileutil

import "os"

// Fsync is a wrapper around file.Sync(). Special handling is needed on darwin platform.
func Fsync(f *os.File) error {
	return f.Sync()
}

// Fdatasync is a wrapper around file.Sync(). Special handling is needed on linux platform.
func Fdatasync(f *os.File) error {
	return f.Sync()
}

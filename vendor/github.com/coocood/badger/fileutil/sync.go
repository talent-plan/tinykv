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

// SyncFileRange does nothing on non linux platform.
func SyncFileRange(f *os.File, offset int64, size int64, async bool) error {
	return nil
}

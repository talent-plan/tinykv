// +build darwin

package fileutil

import (
	"golang.org/x/sys/unix"
	"os"
)

// Fsync on HFS/OSX flushes the data on to the physical drive but the drive
// may not write it to the persistent media for quite sometime and it may be
// written in out-of-order sequence. Using F_FULLFSYNC ensures that the
// physical drive's buffer will also get flushed to the media.
func Fsync(f *os.File) error {
	_, err := unix.FcntlInt(f.Fd(), unix.F_FULLFSYNC, 0)
	return err
}

// Fdatasync on darwin platform invokes fcntl(F_FULLFSYNC) for actual persistence
// on physical drive media.
func Fdatasync(f *os.File) error {
	return Fsync(f)
}

// SyncFileRange does nothing on non linux platform.
func SyncFileRange(f *os.File, offset int64, size int64, async bool) error {
	return nil
}

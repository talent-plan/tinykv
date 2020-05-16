// +build darwin

package fileutil

import (
	"os"
	"syscall"
)

// Fsync on darwin platform flushes the data on to the physical drive but the drive
// may not write it to the persistent media for quite sometime and it may be
// written in out-of-order sequence.
func Fsync(f *os.File) error {
	err := syscall.Fsync(int(f.Fd()))
	return err
}

// Fdatasync is the same as Fsync on darwin platform.
func Fdatasync(f *os.File) error {
	return Fsync(f)
}

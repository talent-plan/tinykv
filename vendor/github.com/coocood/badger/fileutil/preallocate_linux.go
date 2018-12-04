// +build linux

package fileutil

import (
	"golang.org/x/sys/unix"
	"os"
)

func preallocate(f *os.File, size int64) error {
	err := unix.Fallocate(int(f.Fd()), 0, 0, size)
	if err != nil {
		errno, ok := err.(unix.Errno)
		if ok && (errno == unix.ENOTSUP || errno == unix.EINTR) {
			return preallocateTrunc(f, size)
		}
	}
	return err
}

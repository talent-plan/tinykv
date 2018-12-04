// +build linux

package fileutil

import (
	"golang.org/x/sys/unix"
	"os"
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

// SyncFileRange use sync_file_range() to flush dirty pages.
// offset is the starting byte of the file range to be synchronized;
// nbytes specifies the length of the range to be synchronized, in bytes.
// If nbytes is zero, then all bytes from offset through to the end of file are synchronized.
// Synchronization is in units of the system page size:
// offset is rounded down to a page boundary; (offset+nbytes-1) is rounded up to a page boundary.
// If async is true, just initiate write-out of all dirty pages in the specified range
// which are not presently submitted write-out. Note that even this may block
// if you attempt to write more than request queue size.
// Otherwise it will wait upon write-out of all pages in the range after performing any write.
//
// Important: unlike Fdatasync, this function will never update file's metadata (size etc.), which means there are no
// guarantees that the data will be available after a crash. Please use Fsync or Fdatasync at the end of file write.
func SyncFileRange(f *os.File, offset int64, nbytes int64, async bool) error {
	flag := unix.SYNC_FILE_RANGE_WRITE
	if !async {
		flag |= unix.SYNC_FILE_RANGE_WAIT_AFTER
	}
	return unix.SyncFileRange(int(f.Fd()), offset, nbytes, flag)
}

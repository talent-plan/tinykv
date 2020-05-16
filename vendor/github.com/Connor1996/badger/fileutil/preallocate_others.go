// +build !linux

package fileutil

import "os"

func preallocate(f *os.File, size int64) error {
	return preallocateTrunc(f, size)
}

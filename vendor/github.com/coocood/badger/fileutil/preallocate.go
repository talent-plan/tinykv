package fileutil

import "os"

func Preallocate(f *os.File, size int64) error {
	if size == 0 {
		return nil
	}
	return preallocate(f, size)
}

func preallocateTrunc(f *os.File, size int64) error {
	return f.Truncate(size)
}

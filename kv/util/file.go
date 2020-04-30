package util

import (
	"hash/crc32"
	"io"
	"os"

	"github.com/pingcap/errors"
)

func GetFileSize(path string) (uint64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return uint64(fi.Size()), nil
}

func FileExists(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !fi.IsDir()
}

func DirExists(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	return fi.IsDir()
}

func DeleteFileIfExists(path string) (bool, error) {
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, errors.WithStack(err)
	}
	return true, nil
}

// CalcCRC32 Calculates the given file's CRC32 checksum.
func CalcCRC32(path string) (uint32, error) {
	digest := crc32.NewIEEE()
	f, err := os.Open(path)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	_, err = io.Copy(digest, f)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return digest.Sum32(), nil
}

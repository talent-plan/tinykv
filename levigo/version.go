package levigo

/*
#cgo LDFLAGS: -lleveldb
#include "leveldb/c.h"
*/
import "C"

// GetLevelDBMajorVersion returns the underlying LevelDB implementation's major
// version.
func GetLevelDBMajorVersion() int {
	return int(C.leveldb_major_version())
}

// GetLevelDBMinorVersion returns the underlying LevelDB implementation's minor
// version.
func GetLevelDBMinorVersion() int {
	return int(C.leveldb_minor_version())
}

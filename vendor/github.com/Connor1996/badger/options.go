/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

import (
	"github.com/Connor1996/badger/options"
)

// NOTE: Keep the comments in the following to 75 chars width, so they
// format nicely in godoc.

// Options are params for creating DB object.
//
// This package provides DefaultOptions which contains options that should
// work for most applications. Consider using that as a starting point before
// customizing it for your own needs.
type Options struct {
	// 1. Mandatory flags
	// -------------------
	// Directory to store the data in. Should exist and be writable.
	Dir string
	// Directory to store the value log in. Can be the same as Dir. Should
	// exist and be writable.
	ValueDir string

	// 2. Frequently modified flags
	// -----------------------------
	// Sync all writes to disk. Setting this to true would slow down data
	// loading significantly.
	SyncWrites bool

	// How should LSM tree be accessed.
	TableLoadingMode options.FileLoadingMode

	// How should value log be accessed.
	ValueLogLoadingMode options.FileLoadingMode

	// 3. Flags that user might want to review
	// ----------------------------------------
	// The following affect all levels of LSM tree.
	MaxTableSize int64 // Each table (or file) is at most this size.
	// If value size >= this threshold, only store value offsets in tree.
	// If set to 0, all values are stored in SST.
	ValueThreshold int
	// Maximum number of tables to keep in memory, before stalling.
	NumMemtables int
	// The following affect how we handle LSM tree L0.
	// Maximum number of Level 0 tables before we start compacting.
	NumLevelZeroTables int

	// If we hit this number of Level 0 tables, we will stall until L0 is
	// compacted away.
	NumLevelZeroTablesStall int

	MaxCacheSize int64

	// Maximum total size for L1.
	LevelOneSize int64

	// Size of single value log file.
	ValueLogFileSize int64

	// Max number of entries a value log file can hold (approximately). A value log file would be
	// determined by the smaller of its file size and max entries.
	ValueLogMaxEntries uint32

	// Max number of value log files to keep before safely remove.
	ValueLogMaxNumFiles int

	// Number of compaction workers to run concurrently.
	NumCompactors int

	// Max number of sub compaction, set 1 or 0 to disable sub compaction.
	MaxSubCompaction int

	// Transaction start and commit timestamps are manaVgedTxns by end-user. This
	// is a private option used by ManagedDB.
	managedTxns bool

	// 4. Flags for testing purposes
	// ------------------------------
	DoNotCompact bool // Stops LSM tree from compactions.

	maxBatchCount int64 // max entries in batch
	maxBatchSize  int64 // max batch size in bytes

	// Open the DB as read-only. With this set, multiple processes can
	// open the same Badger DB. Note: if the DB being opened had crashed
	// before and has vlog data to be replayed, ReadOnly will cause Open
	// to fail with an appropriate message.
	ReadOnly bool

	// Truncate value log to delete corrupt data, if any. Would not truncate if ReadOnly is set.
	Truncate bool

	TableBuilderOptions options.TableBuilderOptions

	ValueLogWriteOptions options.ValueLogWriterOptions

	CompactionFilterFactory func(targetLevel int, smallest, biggest []byte) CompactionFilter
}

// CompactionFilter is an interface that user can implement to remove certain keys.
type CompactionFilter interface {
	// Filter is the method the compaction process invokes for kv that is being compacted. The returned decision
	// indicates that the kv should be preserved, deleted or dropped in the output of this compaction run.
	Filter(key, val, userMeta []byte) Decision

	// Guards returns specifications that may splits the SST files
	// A key is associated to a guard that has the longest matched Prefix.
	Guards() []Guard
}

// Guard specifies when to finish a SST file during compaction. The rule is the following:
// 1. The key must match the Prefix of the Guard, otherwise the SST should finish.
// 2. If the key up to MatchLen is the different than the previous key and MinSize is reached, the SST should finish.
type Guard struct {
	Prefix   []byte
	MatchLen int
	MinSize  int64
}

// Decision is the type for compaction filter decision.
type Decision int

const (
	// DecisionKeep indicates the entry should be reserved.
	DecisionKeep Decision = 0
	// DecisionMarkTombstone converts the entry to a delete tombstone.
	DecisionMarkTombstone Decision = 1
	// DecisionDrop simply drops the entry, doesn't leave a delete tombstone.
	DecisionDrop Decision = 2
)

// DefaultOptions sets a list of recommended options for good performance.
// Feel free to modify these to suit your needs.
var DefaultOptions = Options{
	DoNotCompact:        false,
	LevelOneSize:        256 << 20,
	TableLoadingMode:    options.LoadToRAM,
	ValueLogLoadingMode: options.FileIO,
	// table.MemoryMap to mmap() the tables.
	// table.Nothing to not preload the tables.
	MaxTableSize:            64 << 20,
	NumCompactors:           3,
	MaxSubCompaction:        3,
	NumLevelZeroTables:      5,
	NumLevelZeroTablesStall: 10,
	NumMemtables:            5,
	SyncWrites:              true,
	ValueLogFileSize:        256 << 20,
	ValueLogMaxEntries:      1000000,
	ValueLogMaxNumFiles:     1,
	ValueThreshold:          32,
	Truncate:                false,
	MaxCacheSize:            1 * 1024 * 1024 * 1024,
	TableBuilderOptions: options.TableBuilderOptions{
		SuRFStartLevel:      8,
		HashUtilRatio:       0.75,
		WriteBufferSize:     2 * 1024 * 1024,
		BytesPerSecond:      -1,
		MaxLevels:           7,
		LevelSizeMultiplier: 10,
		BlockSize:           4 * 1024,
		Compression:         options.ZSTD,
		LogicalBloomFPR:     0.01,
		SuRFOptions: options.SuRFOptions{
			HashSuffixLen:  8,
			RealSuffixLen:  8,
			BitsPerKeyHint: 40,
		},
	},
	ValueLogWriteOptions: options.ValueLogWriterOptions{
		WriteBufferSize: 2 * 1024 * 1024,
	},
}

// LSMOnlyOptions follows from DefaultOptions, but sets a higher ValueThreshold so values would
// be colocated with the LSM tree, with value log largely acting as a write-ahead log only. These
// options would reduce the disk usage of value log, and make Badger act like a typical LSM tree.
var LSMOnlyOptions = Options{}

func init() {
	LSMOnlyOptions = DefaultOptions

	LSMOnlyOptions.ValueThreshold = 65500      // Max value length which fits in uint16.
	LSMOnlyOptions.ValueLogFileSize = 64 << 20 // Allow easy space reclamation.
	LSMOnlyOptions.ValueLogLoadingMode = options.FileIO
}

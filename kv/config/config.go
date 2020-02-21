package config

import (
	"time"

	"github.com/coocood/badger/options"
	"github.com/ngaut/log"
)

type Config struct {
	Server      Server      `toml:"server"`      // Unistore server options
	Engine      Engine      `toml:"engine"`      // Engine options.
	RaftStore   RaftStore   `toml:"raftstore"`   // RaftStore configs
	Coprocessor Coprocessor `toml:"coprocessor"` // Coprocessor options
}

type Server struct {
	PDAddr     string `toml:"pd-addr"`
	StoreAddr  string `toml:"store-addr"`
	StatusAddr string `toml:"status-addr"`
	LogLevel   string `toml:"log-level"`
	RegionSize int64  `toml:"region-size"` // Average region size.
	MaxProcs   int    `toml:"max-procs"`   // Max CPU cores to use, set 0 to use all CPU cores in the machine.
	Raft       bool   `toml:"raft"`        // Enable raft.
}

type RaftStore struct {
	PdHeartbeatTickInterval  string `toml:"pd-heartbeat-tick-interval"`  // pd-heartbeat-tick-interval in seconds
	RaftStoreMaxLeaderLease  string `toml:"raft-store-max-leader-lease"` // raft-store-max-leader-lease in milliseconds
	RaftBaseTickInterval     string `toml:"raft-base-tick-interval"`     // raft-base-tick-interval in milliseconds
	RaftHeartbeatTicks       int    `toml:"raft-heartbeat-ticks"`        // raft-heartbeat-ticks times
	RaftElectionTimeoutTicks int    `toml:"raft-election-timeout-ticks"` // raft-election-timeout-ticks times
}

type Coprocessor struct {
	RegionMaxKeys   int64 `toml:"region-max-keys"`
	RegionSplitKeys int64 `toml:"region-split-keys"`
}

type Engine struct {
	DBPath           string `toml:"db-path"`             // Directory to store the data in. Should exist and be writable.
	ValueThreshold   int    `toml:"value-threshold"`     // If value size >= this threshold, only store value offsets in tree.
	MaxTableSize     int64  `toml:"max-table-size"`      // Each table is at most this size.
	NumMemTables     int    `toml:"num-mem-tables"`      // Maximum number of tables to keep in memory, before stalling.
	NumL0Tables      int    `toml:"num-L0-tables"`       // Maximum number of Level 0 tables before we start compacting.
	NumL0TablesStall int    `toml:"num-L0-tables-stall"` // Maximum number of Level 0 tables before stalling.
	VlogFileSize     int64  `toml:"vlog-file-size"`      // Value log file size.

	// 	Sync all writes to disk. Setting this to true would slow down data loading significantly.")
	SyncWrite         bool     `toml:"sync-write"`
	NumCompactors     int      `toml:"num-compactors"`
	SurfStartLevel    int      `toml:"surf-start-level"`
	BlockCacheSize    int64    `toml:"block-cache-size"`
	Compression       []string `toml:"compression"` // Compression types for each level
	IngestCompression string   `toml:"ingest-compression"`
}

func ParseCompression(s string) options.CompressionType {
	switch s {
	case "snappy":
		return options.Snappy
	case "zstd":
		return options.ZSTD
	default:
		return options.None
	}
}

const MB = 1024 * 1024

var DefaultConf = Config{
	Server: Server{
		PDAddr:     "127.0.0.1:2379",
		StoreAddr:  "127.0.0.1:9191",
		StatusAddr: "127.0.0.1:9291",
		RegionSize: 64 * MB,
		LogLevel:   "info",
		MaxProcs:   0,
		Raft:       true,
	},
	RaftStore: RaftStore{
		PdHeartbeatTickInterval:  "20s",
		RaftStoreMaxLeaderLease:  "9s",
		RaftBaseTickInterval:     "1s",
		RaftHeartbeatTicks:       2,
		RaftElectionTimeoutTicks: 10,
	},
	Engine: Engine{
		DBPath:           "/tmp/badger",
		ValueThreshold:   256,
		MaxTableSize:     64 * MB,
		NumMemTables:     3,
		NumL0Tables:      4,
		NumL0TablesStall: 8,
		VlogFileSize:     256 * MB,
		SyncWrite:        true,
		NumCompactors:    1,
		SurfStartLevel:   8,
		Compression:      make([]string, 7),
		BlockCacheSize:   1 << 30,
	},
}

// parseDuration parses duration argument string.
func ParseDuration(durationStr string) time.Duration {
	dur, err := time.ParseDuration(durationStr)
	if err != nil {
		dur, err = time.ParseDuration(durationStr + "s")
	}
	if err != nil || dur < 0 {
		log.Fatalf("invalid duration=%v", durationStr)
	}
	return dur
}

var globalConf = DefaultConf

func GetGlobalConf() *Config {
	return &globalConf
}

func SetGlobalConf(c *Config) {
	globalConf = *c
}

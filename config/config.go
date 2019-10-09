package config

type Config struct {
	PDAddr      string `toml:"pd-addr"`
	StoreAddr   string `toml:"store-addr"`
	HttpAddr    string `toml:"http-addr"`
	LogLevel    string `toml:"log-level"`
	RegionSize  int64  `toml:"region-size"`  // Average region size.
	MaxProcs    int    `toml:"max-procs"`    // Max CPU cores to use, set 0 to use all CPU cores in the machine.
	Raft        bool   `toml:"raft"`         // Enable raft.
	RaftWorkers int    `toml:"raft-workers"` // Number of raft workers.
	Engine      Engine `toml:"engine"`       // Engine options.
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
	SyncWrite     bool `toml:"sync-write"`
	HashIndex     bool `toml:"hash-index"` // Enable hash index
	NumCompactors int  `toml:"num-compactors"`
}

const MB = 1024 * 1024

var DefaultConf = Config{
	PDAddr:      "127.0.0.1:2379",
	StoreAddr:   "127.0.0.1:9191",
	HttpAddr:    "127.0.0.1:9291",
	RegionSize:  64 * MB,
	LogLevel:    "info",
	MaxProcs:    0,
	Raft:        true,
	RaftWorkers: 2,
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
	},
}

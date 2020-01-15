package engine_util

import (
	"github.com/coocood/badger"
)

type Engines struct {
	kv       *badger.DB
	kvPath   string
	raft     *badger.DB
	raftPath string
}

func NewEngines(kvEngine, raftEngine *badger.DB, kvPath, raftPath string) *Engines {
	return &Engines{
		kv:       kvEngine,
		kvPath:   kvPath,
		raft:     raftEngine,
		raftPath: raftPath,
	}
}

func (en *Engines) WriteKV(wb *WriteBatch) error {
	return wb.WriteToKV(en.kv)
}

func (en *Engines) WriteRaft(wb *WriteBatch) error {
	return wb.WriteToRaft(en.raft)
}

func (en *Engines) SyncKVWAL() error {
	// TODO: implement
	return nil
}

func (en *Engines) SyncRaftWAL() error {
	// TODO: implement
	return nil
}

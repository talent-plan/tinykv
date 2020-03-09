package util

import (
	"io/ioutil"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

func NewTestEngines() *engine_util.Engines {
	engines := new(engine_util.Engines)
	var err error
	engines.KvPath, err = ioutil.TempDir("", "tinykv_kv")
	if err != nil {
		panic("create kv dir failed")
	}
	kvOpts := badger.DefaultOptions
	kvOpts.Dir = engines.KvPath
	kvOpts.ValueDir = engines.KvPath
	kvOpts.ValueThreshold = 256
	engines.Kv, err = badger.Open(kvOpts)
	if err != nil {
		panic("open kv db failed")
	}
	engines.RaftPath, err = ioutil.TempDir("", "tinykv_raft")
	if err != nil {
		panic("create raft dir failed")
	}
	raftOpts := badger.DefaultOptions
	raftOpts.Dir = engines.RaftPath
	raftOpts.ValueDir = engines.RaftPath
	raftOpts.ValueThreshold = 256
	engines.Raft, err = badger.Open(raftOpts)
	if err != nil {
		panic("open raft db failed")
	}
	return engines
}

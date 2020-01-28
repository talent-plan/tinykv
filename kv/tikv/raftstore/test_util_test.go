package raftstore

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/coocood/badger"
	"github.com/pingcap-incubator/tinykv/kv/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/stretchr/testify/require"
)

func newTestEngines(t *testing.T) *engine_util.Engines {
	engines := new(engine_util.Engines)
	var err error
	engines.KvPath, err = ioutil.TempDir("", "tinykv_kv")
	require.Nil(t, err)
	kvOpts := badger.DefaultOptions
	kvOpts.Dir = engines.KvPath
	kvOpts.ValueDir = engines.KvPath
	kvOpts.ValueThreshold = 256
	engines.Kv, err = badger.Open(kvOpts)
	require.Nil(t, err)
	engines.RaftPath, err = ioutil.TempDir("", "tinykv_raft")
	require.Nil(t, err)
	raftOpts := badger.DefaultOptions
	raftOpts.Dir = engines.RaftPath
	raftOpts.ValueDir = engines.RaftPath
	raftOpts.ValueThreshold = 256
	engines.Raft, err = badger.Open(raftOpts)
	require.Nil(t, err)
	return engines
}

func newTestPeerStorage(t *testing.T) *PeerStorage {
	engines := newTestEngines(t)
	err := BootstrapStore(engines, 1, 1)
	require.Nil(t, err)
	region, err := PrepareBootstrap(engines, 1, 1, 1)
	require.Nil(t, err)
	peerStore, err := NewPeerStorage(engines, region, nil, 1, "")
	require.Nil(t, err)
	return peerStore
}

func newTestPeerStorageFromEnts(t *testing.T, ents []eraftpb.Entry) *PeerStorage {
	peerStore := newTestPeerStorage(t)
	kvWB := new(engine_util.WriteBatch)
	ctx := NewInvokeContext(peerStore)
	raftWB := new(engine_util.WriteBatch)
	require.Nil(t, peerStore.Append(ctx, ents[1:], raftWB))
	ctx.ApplyState.truncatedIndex = ents[0].Index
	ctx.ApplyState.truncatedTerm = ents[0].Term
	ctx.ApplyState.appliedIndex = ents[len(ents)-1].Index
	ctx.saveApplyStateTo(kvWB)
	require.Nil(t, peerStore.Engines.WriteRaft(raftWB))
	peerStore.Engines.WriteKV(kvWB)
	peerStore.raftState = ctx.RaftState
	peerStore.applyState = ctx.ApplyState
	return peerStore
}

func cleanUpTestData(peerStore *PeerStorage) {
	os.RemoveAll(peerStore.Engines.KvPath)
	os.RemoveAll(peerStore.Engines.RaftPath)
}

func cleanUpTestEngineData(engines *engine_util.Engines) {
	os.RemoveAll(engines.KvPath)
	os.RemoveAll(engines.RaftPath)
}

func newTestEntry(index, term uint64) eraftpb.Entry {
	return eraftpb.Entry{
		Index: index,
		Term:  term,
		Data:  []byte{0},
	}
}

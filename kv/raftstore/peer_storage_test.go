package raftstore

import (
	"bytes"
	"testing"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestPeerStorage(t *testing.T) *PeerStorage {
	engines := util.NewTestEngines()
	err := BootstrapStore(engines, 1, 1)
	require.Nil(t, err)
	region, err := PrepareBootstrap(engines, 1, 1, 1)
	require.Nil(t, err)
	peerStore, err := NewPeerStorage(engines, region, nil, "")
	require.Nil(t, err)
	return peerStore
}

func newTestPeerStorageFromEnts(t *testing.T, ents []eraftpb.Entry) *PeerStorage {
	peerStore := newTestPeerStorage(t)
	kvWB := new(engine_util.WriteBatch)
	raftWB := new(engine_util.WriteBatch)
	require.Nil(t, peerStore.Append(ents[1:], raftWB))
	applyState := peerStore.applyState
	applyState.TruncatedState = &rspb.RaftTruncatedState{
		Index: ents[0].Index,
		Term:  ents[0].Term,
	}
	applyState.AppliedIndex = ents[len(ents)-1].Index
	kvWB.SetMeta(meta.ApplyStateKey(peerStore.region.GetId()), applyState)
	require.Nil(t, peerStore.Engines.WriteRaft(raftWB))
	peerStore.Engines.WriteKV(kvWB)
	return peerStore
}

func cleanUpTestData(peerStore *PeerStorage) {
	if err := peerStore.Engines.Destroy(); err != nil {
		panic(err)
	}
}

func newTestEntry(index, term uint64) eraftpb.Entry {
	return eraftpb.Entry{
		Index: index,
		Term:  term,
		Data:  []byte{0},
	}
}

func TestPeerStorageTerm(t *testing.T) {
	ents := []eraftpb.Entry{
		newTestEntry(6, 6), newTestEntry(7, 7), newTestEntry(8, 8),
	}
	tests := []struct {
		idx  uint64
		term uint64
		err  error
	}{
		{5, 0, raft.ErrCompacted},
		{6, 6, nil},
		{7, 7, nil},
		{8, 8, nil},
	}
	for _, tt := range tests {
		peerStore := newTestPeerStorageFromEnts(t, ents)
		term, err := peerStore.Term(tt.idx)
		if err != nil {
			assert.Equal(t, tt.err, err)
		} else {
			assert.Equal(t, tt.term, term)
		}
		cleanUpTestData(peerStore)
	}
}

func appendEnts(t *testing.T, peerStore *PeerStorage, ents []eraftpb.Entry) {
	raftWB := new(engine_util.WriteBatch)
	require.Nil(t, peerStore.Append(ents, raftWB))
	raftWB.SetMeta(meta.RaftStateKey(peerStore.region.GetId()), peerStore.raftState)
	require.Nil(t, peerStore.Engines.WriteRaft(raftWB))
}

func getMetaKeyCount(t *testing.T, peerStore *PeerStorage) int {
	regionID := peerStore.region.Id
	count := 0
	metaStart := meta.RegionMetaPrefixKey(regionID)
	metaEnd := meta.RegionMetaPrefixKey(regionID + 1)
	err := peerStore.Engines.Kv.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(metaStart); it.Valid(); it.Next() {
			if bytes.Compare(it.Item().Key(), metaEnd) >= 0 {
				break
			}
			count++
		}
		return nil
	})
	require.Nil(t, err)
	raftStart := meta.RegionRaftPrefixKey(regionID)
	raftEnd := meta.RegionRaftPrefixKey(regionID + 1)
	err = peerStore.Engines.Kv.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(metaStart); it.Valid(); it.Next() {
			if bytes.Compare(it.Item().Key(), metaEnd) >= 0 {
				break
			}
			count++
		}
		return nil
	})
	require.Nil(t, err)
	err = peerStore.Engines.Raft.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(raftStart); it.Valid(); it.Next() {
			if bytes.Compare(it.Item().Key(), raftEnd) >= 0 {
				break
			}
			count++
		}
		return nil
	})
	require.Nil(t, err)
	return count
}

func TestPeerStorageClearMeta(t *testing.T) {
	peerStore := newTestPeerStorageFromEnts(t, []eraftpb.Entry{
		newTestEntry(3, 3),
		newTestEntry(4, 4),
	})
	defer cleanUpTestData(peerStore)
	appendEnts(t, peerStore, []eraftpb.Entry{
		newTestEntry(5, 5),
		newTestEntry(6, 6),
	})
	assert.Equal(t, 6, getMetaKeyCount(t, peerStore))
	kvWB := new(engine_util.WriteBatch)
	raftWB := new(engine_util.WriteBatch)
	require.Nil(t, peerStore.clearMeta(kvWB, raftWB))
	require.Nil(t, peerStore.Engines.WriteKV(kvWB))
	require.Nil(t, peerStore.Engines.WriteRaft(raftWB))
	assert.Equal(t, 0, getMetaKeyCount(t, peerStore))
}

func TestPeerStorageEntries(t *testing.T) {
	ents := []eraftpb.Entry{
		newTestEntry(6, 6),
		newTestEntry(7, 7),
		newTestEntry(8, 8),
		newTestEntry(9, 9),
	}
	tests := []struct {
		low     uint64
		high    uint64
		entries []eraftpb.Entry
		err     error
	}{
		{5, 9, nil, raft.ErrCompacted},
		{6, 7, nil, raft.ErrCompacted},
		{7, 8, []eraftpb.Entry{
			newTestEntry(7, 7),
		}, nil},
		{7, 9, []eraftpb.Entry{
			newTestEntry(7, 7),
			newTestEntry(8, 8),
		}, nil},
	}

	for i, tt := range tests {
		peerStore := newTestPeerStorageFromEnts(t, ents)
		defer cleanUpTestData(peerStore)
		entries, err := peerStore.Entries(tt.low, tt.high)
		if err != nil {
			assert.Equal(t, tt.err, err)
		} else {
			assert.Equal(t, tt.entries, entries, "%d", i)
		}
	}
}

func TestPeerStorageAppend(t *testing.T) {
	ents := []eraftpb.Entry{
		newTestEntry(6, 6), newTestEntry(7, 7), newTestEntry(8, 8)}
	tests := []struct {
		appends []eraftpb.Entry
		results []eraftpb.Entry
	}{
		{
			[]eraftpb.Entry{newTestEntry(6, 6), newTestEntry(7, 7), newTestEntry(8, 8)},
			[]eraftpb.Entry{newTestEntry(7, 7), newTestEntry(8, 8)},
		},
		{
			[]eraftpb.Entry{newTestEntry(6, 6), newTestEntry(7, 9), newTestEntry(8, 9)},
			[]eraftpb.Entry{newTestEntry(7, 9), newTestEntry(8, 9)},
		},
		{
			[]eraftpb.Entry{
				newTestEntry(6, 6),
				newTestEntry(7, 7),
				newTestEntry(8, 8),
				newTestEntry(9, 8),
			},
			[]eraftpb.Entry{newTestEntry(7, 7), newTestEntry(8, 8), newTestEntry(9, 8)},
		},
		// truncate incoming entries, truncate the existing entries and append
		{
			[]eraftpb.Entry{newTestEntry(5, 6), newTestEntry(6, 6), newTestEntry(7, 8)},
			[]eraftpb.Entry{newTestEntry(7, 8)},
		},
		// truncate the existing entries and append
		{[]eraftpb.Entry{newTestEntry(7, 8)}, []eraftpb.Entry{newTestEntry(7, 8)}},
		// direct append
		{
			[]eraftpb.Entry{newTestEntry(9, 8)},
			[]eraftpb.Entry{newTestEntry(7, 7), newTestEntry(8, 8), newTestEntry(9, 8)},
		},
	}
	for _, tt := range tests {
		peerStore := newTestPeerStorageFromEnts(t, ents)
		defer cleanUpTestData(peerStore)
		appendEnts(t, peerStore, tt.appends)
		li := peerStore.raftState.LastIndex
		acutualEntries, err := peerStore.Entries(7, li+1)
		require.Nil(t, err)
		assert.Equal(t, tt.results, acutualEntries)
	}
}

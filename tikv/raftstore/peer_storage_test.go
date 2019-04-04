package raftstore

import (
	"bytes"
	"math"
	"testing"

	"github.com/coocood/badger"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zhangjinpeng1987/raft"
)

func TestPeerStorageTerm(t *testing.T) {
	ents := []eraftpb.Entry{
		newTestEntry(3, 3), newTestEntry(4, 4), newTestEntry(5, 5),
	}
	tests := []struct {
		idx  uint64
		term uint64
		err  error
	}{
		{2, 0, raft.ErrCompacted},
		{3, 3, nil},
		{4, 4, nil},
		{5, 5, nil},
	}
	for _, tt := range tests {
		peerStore := newTestPeerStorageFromEnts(t, ents)
		term, err := peerStore.Term(tt.idx)
		if err != nil {
			assert.Equal(t, err, tt.err)
		} else {
			assert.Equal(t, term, tt.term)
		}
		cleanUpTestData(peerStore)
	}
}

func appendEnts(t *testing.T, peerStore *PeerStorage, ents []eraftpb.Entry) {
	ctx := NewInvokeContext(peerStore)
	readyCtx := new(readyContext)
	require.Nil(t, peerStore.Append(ctx, ents, readyCtx))
	ctx.saveRaftStateTo(readyCtx.RaftWB())
	require.Nil(t, peerStore.Engines.WriteRaft(readyCtx.RaftWB()))
	peerStore.raftState = ctx.RaftState
}

func validateCache(t *testing.T, peerStore *PeerStorage, expEnts []eraftpb.Entry) {
	assert.Equal(t, peerStore.cache.cache, expEnts)
	for _, e := range expEnts {
		key := RaftLogKey(peerStore.region.Id, e.Index)
		e2 := new(eraftpb.Entry)
		assert.Nil(t, getMsg(peerStore.Engines.raft, key, e2))
		assert.Equal(t, *e2, e)
	}
}

func getMetaKeyCount(t *testing.T, peerStore *PeerStorage) int {
	regionID := peerStore.region.Id
	count := 0
	metaStart := RegionMetaPrefixKey(regionID)
	metaEnd := RegionMetaPrefixKey(regionID + 1)
	err := peerStore.Engines.kv.db.View(func(txn *badger.Txn) error {
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
	raftStart := RegionRaftPrefixKey(regionID)
	raftEnd := RegionRaftPrefixKey(regionID + 1)
	err = peerStore.Engines.kv.db.View(func(txn *badger.Txn) error {
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
	err = peerStore.Engines.raft.View(func(txn *badger.Txn) error {
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
	kvWB := new(WriteBatch)
	raftWB := new(WriteBatch)
	require.Nil(t, peerStore.clearMeta(kvWB, raftWB))
	require.Nil(t, peerStore.Engines.WriteKV(kvWB))
	require.Nil(t, peerStore.Engines.WriteRaft(raftWB))
	assert.Equal(t, 0, getMetaKeyCount(t, peerStore))
}

func TestPeerStorageEntries(t *testing.T) {
	ents := []eraftpb.Entry{
		newTestEntry(3, 3),
		newTestEntry(4, 4),
		newTestEntry(5, 5),
		newTestEntry(6, 6),
	}
	tests := []struct {
		low     uint64
		high    uint64
		maxSize uint64
		entries []eraftpb.Entry
		err     error
	}{
		{2, 6, math.MaxUint64, nil, raft.ErrCompacted},
		{3, 4, math.MaxUint64, nil, raft.ErrCompacted},
		{4, 5, math.MaxUint64, []eraftpb.Entry{
			newTestEntry(4, 4),
		}, nil},
		{4, 6, math.MaxUint64, []eraftpb.Entry{
			newTestEntry(4, 4),
			newTestEntry(5, 5),
		}, nil},
		// even if maxsize is zero, the first entry should be returned
		{4, 7, 0, []eraftpb.Entry{
			newTestEntry(4, 4),
		}, nil},
		// limit to 2
		{4, 7, uint64(ents[1].Size() + ents[2].Size()), []eraftpb.Entry{
			newTestEntry(4, 4),
			newTestEntry(5, 5),
		}, nil},
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size()/2), []eraftpb.Entry{
			newTestEntry(4, 4),
			newTestEntry(5, 5),
		}, nil},
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size() - 1), []eraftpb.Entry{
			newTestEntry(4, 4),
			newTestEntry(5, 5),
		}, nil},
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size()), []eraftpb.Entry{
			newTestEntry(4, 4),
			newTestEntry(5, 5),
			newTestEntry(6, 6),
		}, nil},
	}

	for i, tt := range tests {
		peerStore := newTestPeerStorageFromEnts(t, ents)
		defer cleanUpTestData(peerStore)
		entries, err := peerStore.Entries(tt.low, tt.high, tt.maxSize)
		if err != nil {
			assert.Equal(t, tt.err, err)
		} else {
			assert.Equal(t, tt.entries, entries, "%d", i)
		}
	}
}

func TestPeerStorageCompact(t *testing.T) {
	ents := []eraftpb.Entry{
		newTestEntry(3, 3), newTestEntry(4, 4), newTestEntry(5, 5)}
	tests := []struct {
		idx uint64
		err error
	}{
		{2, raft.ErrCompacted},
		{3, raft.ErrCompacted},
		{4, nil},
		{5, nil},
	}
	peerStore := newTestPeerStorageFromEnts(t, ents)
	defer cleanUpTestData(peerStore)
	for _, tt := range tests {
		ctx := NewInvokeContext(peerStore)
		term, err := peerStore.Term(tt.idx)
		if err == nil {
			err = CompactRaftLog(peerStore.Tag, &ctx.ApplyState, tt.idx, term)
		}
		if tt.err == nil {
			assert.Nil(t, err)
			kvWB := new(WriteBatch)
			ctx.saveApplyStateTo(kvWB)
			require.Nil(t, peerStore.Engines.WriteKV(kvWB))
		} else {
			assert.NotNil(t, err)
		}
	}
}

func TestPeerStorageAppend(t *testing.T) {
	ents := []eraftpb.Entry{
		newTestEntry(3, 3), newTestEntry(4, 4), newTestEntry(5, 5)}
	tests := []struct {
		appends []eraftpb.Entry
		results []eraftpb.Entry
	}{
		{
			[]eraftpb.Entry{newTestEntry(3, 3), newTestEntry(4, 4), newTestEntry(5, 5)},
			[]eraftpb.Entry{newTestEntry(4, 4), newTestEntry(5, 5)},
		},
		{
			[]eraftpb.Entry{newTestEntry(3, 3), newTestEntry(4, 6), newTestEntry(5, 6)},
			[]eraftpb.Entry{newTestEntry(4, 6), newTestEntry(5, 6)},
		},
		{
			[]eraftpb.Entry{
				newTestEntry(3, 3),
				newTestEntry(4, 4),
				newTestEntry(5, 5),
				newTestEntry(6, 5),
			},
			[]eraftpb.Entry{newTestEntry(4, 4), newTestEntry(5, 5), newTestEntry(6, 5)},
		},
		// truncate incoming entries, truncate the existing entries and append
		{
			[]eraftpb.Entry{newTestEntry(2, 3), newTestEntry(3, 3), newTestEntry(4, 5)},
			[]eraftpb.Entry{newTestEntry(4, 5)},
		},
		// truncate the existing entries and append
		{[]eraftpb.Entry{newTestEntry(4, 5)}, []eraftpb.Entry{newTestEntry(4, 5)}},
		// direct append
		{
			[]eraftpb.Entry{newTestEntry(6, 5)},
			[]eraftpb.Entry{newTestEntry(4, 4), newTestEntry(5, 5), newTestEntry(6, 5)},
		},
	}
	for _, tt := range tests {
		peerStore := newTestPeerStorageFromEnts(t, ents)
		defer cleanUpTestData(peerStore)
		appendEnts(t, peerStore, tt.appends)
		li := peerStore.raftState.lastIndex
		acutualEntries, err := peerStore.Entries(4, li+1, math.MaxUint64)
		require.Nil(t, err)
		assert.Equal(t, tt.results, acutualEntries)
	}
}

func TestPeerStorageCacheFetch(t *testing.T) {
	ents := []eraftpb.Entry{
		newTestEntry(3, 3), newTestEntry(4, 4), newTestEntry(5, 5)}
	peerStore := newTestPeerStorageFromEnts(t, ents)
	defer cleanUpTestData(peerStore)
	peerStore.cache.cache = nil
	// empty cache should fetch data from engine directly.
	fetched, err := peerStore.Entries(4, 6, math.MaxUint64)
	require.Nil(t, err)
	assert.Equal(t, fetched, ents[1:])

	entries := []eraftpb.Entry{newTestEntry(6, 5), newTestEntry(7, 5)}
	appendEnts(t, peerStore, entries)
	validateCache(t, peerStore, entries)

	// direct cache access
	fetched, err = peerStore.Entries(6, 8, math.MaxUint64)
	assert.Nil(t, err)
	assert.Equal(t, entries, fetched)

	// size limit should be supported correctly.
	fetched, err = peerStore.Entries(4, 8, 0)
	assert.Nil(t, err)
	assert.Equal(t, []eraftpb.Entry{newTestEntry(4, 4)}, fetched)
	var size uint64
	for _, e := range ents[1:] {
		size += uint64(e.Size())
	}
	fetched, err = peerStore.Entries(4, 8, size)
	assert.Nil(t, err)
	var expRes []eraftpb.Entry
	expRes = append(expRes, ents[1:]...)
	assert.Equal(t, expRes, fetched)
	for _, e := range entries {
		size += uint64(e.Size())
		expRes = append(expRes, e)
		fetched, err = peerStore.Entries(4, 8, size)
		assert.Nil(t, err)
		assert.Equal(t, expRes, fetched)
	}

	// range limit should be supported correctly.
	for low := uint64(4); low < 9; low++ {
		for high := low; high < 9; high++ {
			fetched, err = peerStore.Entries(low, high, math.MaxUint64)
			assert.Equal(t, expRes[low-4:high-4], fetched)
		}
	}
}

func TestPeerStorageCacheUpdate(t *testing.T) {
	ents := []eraftpb.Entry{
		newTestEntry(3, 3), newTestEntry(4, 4), newTestEntry(5, 5)}
	peerStore := newTestPeerStorageFromEnts(t, ents)
	defer cleanUpTestData(peerStore)
	peerStore.cache.cache = nil

	// initial cache
	entries := []eraftpb.Entry{newTestEntry(6, 5), newTestEntry(7, 5)}
	appendEnts(t, peerStore, entries)
	validateCache(t, peerStore, entries)

	// rewrite
	entries = []eraftpb.Entry{newTestEntry(6, 6), newTestEntry(7, 6)}
	appendEnts(t, peerStore, entries)
	validateCache(t, peerStore, entries)

	// rewrite old entry
	entries = []eraftpb.Entry{newTestEntry(5, 6), newTestEntry(6, 6)}
	appendEnts(t, peerStore, entries)
	validateCache(t, peerStore, entries)

	// partial rewrite
	entries = []eraftpb.Entry{newTestEntry(6, 7), newTestEntry(7, 7)}
	appendEnts(t, peerStore, entries)
	expRes := []eraftpb.Entry{newTestEntry(5, 6), newTestEntry(6, 7), newTestEntry(7, 7)}
	validateCache(t, peerStore, expRes)

	// direct append
	entries = []eraftpb.Entry{newTestEntry(8, 7), newTestEntry(9, 7)}
	appendEnts(t, peerStore, entries)
	expRes = append(expRes, entries...)
	validateCache(t, peerStore, expRes)

	// rewrite middle
	entries = []eraftpb.Entry{newTestEntry(7, 8)}
	appendEnts(t, peerStore, entries)
	expRes = expRes[:2]
	expRes = append(expRes, newTestEntry(7, 8))
	validateCache(t, peerStore, expRes)

	capacity := uint64(MaxCacheCapacity)

	// result overflow
	entries = entries[:0]
	for i := uint64(3); i <= capacity; i++ {
		entries = append(entries, newTestEntry(i+5, 8))
	}
	appendEnts(t, peerStore, entries)
	expRes = append(expRes[1:], entries...)
	validateCache(t, peerStore, expRes)

	// input overflow
	entries = entries[:0]
	for i := uint64(0); i <= capacity; i++ {
		entries = append(entries, newTestEntry(i+capacity+6, 8))
	}
	appendEnts(t, peerStore, entries)
	expRes = entries[len(entries)-int(capacity):]
	validateCache(t, peerStore, expRes)

	// compact
	peerStore.CompactTo(capacity + 10)
	expRes = expRes[:0]
	for i := capacity + 10; i < capacity*2+7; i++ {
		expRes = append(expRes, newTestEntry(i, 8))
	}
	validateCache(t, peerStore, expRes)

	// We do not use VecDeque, so no need to test shrink.
	appendEnts(t, peerStore, []eraftpb.Entry{newTestEntry(capacity, 8)})

	// compact all
	peerStore.CompactTo(capacity + 2)
	validateCache(t, peerStore, []eraftpb.Entry{})
	// invalid compaction should be ignored.
	peerStore.CompactTo(capacity)
}

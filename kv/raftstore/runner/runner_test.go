package runner

import (
	"encoding/binary"
	"io"
	"io/ioutil"
	"testing"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// copySnapshot is a helper function to copy snapshot.
// Only used in tests.
func copySnapshot(to, from snap.Snapshot) error {
	if !to.Exists() {
		_, err := io.Copy(to, from)
		if err != nil {
			return errors.WithStack(err)
		}
		return to.Save()
	}
	return nil
}

func newEnginesWithKVDb(t *testing.T, kv *badger.DB) *engine_util.Engines {
	engines := new(engine_util.Engines)
	engines.Kv = kv
	var err error
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

func getTestDBForRegions(t *testing.T, path string, regions []uint64) *badger.DB {
	db := openDB(t, path)
	fillDBData(t, db)
	for _, regionID := range regions {
		// Put apply state into kv engine.
		applyState := &rspb.RaftApplyState{
			AppliedIndex: 10,
			TruncatedState: &rspb.RaftTruncatedState{
				Index: 10,
			},
		}
		require.Nil(t, engine_util.PutMeta(db, meta.ApplyStateKey(regionID), applyState))

		// Put region info into kv engine.
		region := genTestRegion(regionID, 1, 1)
		regionState := new(rspb.RegionLocalState)
		regionState.Region = region
		require.Nil(t, engine_util.PutMeta(db, meta.RegionStateKey(regionID), regionState))
	}
	return db
}

func genTestRegion(regionID, storeID, peerID uint64) *metapb.Region {
	return &metapb.Region{
		Id:       regionID,
		StartKey: []byte(""),
		EndKey:   []byte(""),
		RegionEpoch: &metapb.RegionEpoch{
			Version: 1,
			ConfVer: 1,
		},
		Peers: []*metapb.Peer{
			{StoreId: storeID, Id: peerID},
		},
	}
}

func openDB(t *testing.T, dir string) *badger.DB {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)
	require.Nil(t, err)
	return db
}

func fillDBData(t *testing.T, db *badger.DB) {
	// write some data for multiple cfs.
	wb := new(engine_util.WriteBatch)
	value := make([]byte, 32)
	wb.SetCF(engine_util.CfDefault, []byte("key"), value)
	wb.SetCF(engine_util.CfWrite, []byte("key"), value)
	wb.SetCF(engine_util.CfLock, []byte("key"), value)
	err := wb.WriteToDB(db)
	require.Nil(t, err)
}

func TestGcRaftLog(t *testing.T) {
	engines := util.NewTestEngines()
	defer engines.Destroy()
	raftDb := engines.Raft
	taskResCh := make(chan raftLogGcTaskRes, 1)
	runner := raftLogGCTaskHandler{taskResCh: taskResCh}

	//  generate raft logs
	regionId := uint64(1)
	raftWb := new(engine_util.WriteBatch)
	for i := uint64(0); i < 100; i++ {
		raftWb.SetMeta(meta.RaftLogKey(regionId, i), &eraftpb.Entry{Data: []byte("entry")})
	}
	raftWb.WriteToDB(raftDb)

	type tempHolder struct {
		raftLogGcTask     worker.Task
		expectedCollected uint64
		nonExistRange     [2]uint64
		existRange        [2]uint64
	}

	tbls := []tempHolder{
		{
			raftLogGcTask: &RaftLogGCTask{
				RaftEngine: raftDb,
				RegionID:   regionId,
				StartIdx:   uint64(0),
				EndIdx:     uint64(10),
			},
			expectedCollected: uint64(10),
			nonExistRange:     [...]uint64{0, 10},
			existRange:        [...]uint64{10, 100},
		},

		{
			raftLogGcTask: &RaftLogGCTask{
				RaftEngine: raftDb,
				RegionID:   regionId,
				StartIdx:   uint64(0),
				EndIdx:     uint64(50),
			},
			expectedCollected: uint64(40),
			nonExistRange:     [...]uint64{0, 50},
			existRange:        [...]uint64{50, 100},
		},

		{
			raftLogGcTask: &RaftLogGCTask{
				RaftEngine: raftDb,
				RegionID:   regionId,
				StartIdx:   uint64(50),
				EndIdx:     uint64(50),
			},
			expectedCollected: uint64(0),
			nonExistRange:     [...]uint64{0, 50},
			existRange:        [...]uint64{50, 100},
		},

		{
			raftLogGcTask: &RaftLogGCTask{
				RaftEngine: raftDb,
				RegionID:   regionId,
				StartIdx:   uint64(50),
				EndIdx:     uint64(60),
			},
			expectedCollected: uint64(10),
			nonExistRange:     [...]uint64{0, 60},
			existRange:        [...]uint64{60, 100},
		},
	}

	for _, h := range tbls {
		runner.Handle(h.raftLogGcTask)
		res := <-taskResCh
		assert.Equal(t, h.expectedCollected, uint64(res))
		raftLogMustNotExist(t, raftDb, 1, h.nonExistRange[0], h.nonExistRange[1])
		raftLogMustExist(t, raftDb, 1, h.existRange[0], h.existRange[1])
	}
}

func raftLogMustNotExist(t *testing.T, db *badger.DB, regionId, startIdx, endIdx uint64) {
	for i := startIdx; i < endIdx; i++ {
		k := meta.RaftLogKey(regionId, i)
		db.View(func(txn *badger.Txn) error {
			_, err := txn.Get(k)
			assert.Equal(t, badger.ErrKeyNotFound, err)
			return nil
		})
	}
}

func raftLogMustExist(t *testing.T, db *badger.DB, regionId, startIdx, endIdx uint64) {
	for i := startIdx; i < endIdx; i++ {
		k := meta.RaftLogKey(regionId, i)
		db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(k)
			assert.Nil(t, err)
			assert.NotNil(t, item)
			return nil
		})
	}
}

func cleanUpTestEngineData(engines *engine_util.Engines) {
	if err := engines.Destroy(); err != nil {
		panic(err)
	}
}

type TaskResRouter struct {
	ch chan<- message.Msg
}

func (r *TaskResRouter) Send(regionID uint64, msg message.Msg) error {
	r.ch <- msg
	return nil
}

func (r *TaskResRouter) SendRaftMessage(msg *rspb.RaftMessage) error {
	return nil
}

func (r *TaskResRouter) SendRaftCommand(req *raft_cmdpb.RaftCmdRequest, cb *message.Callback) error {
	return nil
}

func encodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(newKey)-8:], ^ts)
	return newKey
}

func TestSplitCheck(t *testing.T) {
	engines := util.NewTestEngines()
	defer cleanUpTestEngineData(engines)
	db := engines.Kv
	taskResCh := make(chan message.Msg, 1)

	runner := &splitCheckHandler{
		engine:  db,
		router:  &TaskResRouter{ch: taskResCh},
		checker: newSizeSplitChecker(100, 50),
	}

	kvWb := new(engine_util.WriteBatch)
	// the length of each kv pair is 21
	kvWb.SetCF(engine_util.CfDefault, encodeKey([]byte("k1"), 1), []byte("entry"))
	kvWb.SetCF(engine_util.CfDefault, encodeKey([]byte("k1"), 2), []byte("entry"))
	kvWb.SetCF(engine_util.CfDefault, encodeKey([]byte("k2"), 1), []byte("entry"))
	kvWb.SetCF(engine_util.CfDefault, encodeKey([]byte("k2"), 2), []byte("entry"))
	kvWb.SetCF(engine_util.CfDefault, encodeKey([]byte("k3"), 3), []byte("entry"))
	kvWb.MustWriteToDB(db)

	task := &SplitCheckTask{
		Region: &metapb.Region{
			StartKey: []byte(""),
			EndKey:   []byte(""),
		},
	}

	runner.Handle(task)
	msg := <-taskResCh
	split, ok := msg.Data.(*message.MsgSplitRegion)
	assert.True(t, ok)
	assert.Equal(t, codec.EncodeBytes([]byte("k2")), split.SplitKey)
}

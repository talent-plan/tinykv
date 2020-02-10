package raftstore

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/pingcap-incubator/tinykv/kv/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/tikv/config"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
)

type EntryBuilder struct {
	entry eraftpb.Entry
	req   raft_cmdpb.RaftCmdRequest
}

func NewEntryBuilder(index uint64, term uint64) *EntryBuilder {
	return &EntryBuilder{
		entry: eraftpb.Entry{
			Index: index,
			Term:  term,
		},
	}
}

func (b *EntryBuilder) get(cf string, key []byte) *EntryBuilder {
	b.req.Requests = append(b.req.Requests, &raft_cmdpb.Request{
		CmdType: raft_cmdpb.CmdType_Get,
		Get: &raft_cmdpb.GetRequest{
			Cf:  cf,
			Key: key,
		}})
	return b
}

func (b *EntryBuilder) snap() *EntryBuilder {
	b.req.Requests = append(b.req.Requests, &raft_cmdpb.Request{
		CmdType: raft_cmdpb.CmdType_Snap,
		Snap:    &raft_cmdpb.SnapRequest{},
	})
	return b
}

func (b *EntryBuilder) put(cf string, key, value []byte) *EntryBuilder {
	b.req.Requests = append(b.req.Requests, &raft_cmdpb.Request{
		CmdType: raft_cmdpb.CmdType_Put,
		Put: &raft_cmdpb.PutRequest{
			Cf:    cf,
			Key:   key,
			Value: value,
		}})
	return b
}

func (b *EntryBuilder) delete(cf string, key []byte) *EntryBuilder {
	b.req.Requests = append(b.req.Requests, &raft_cmdpb.Request{
		CmdType: raft_cmdpb.CmdType_Delete,
		Delete: &raft_cmdpb.DeleteRequest{
			Cf:  cf,
			Key: key,
		}})
	return b
}

func (b *EntryBuilder) epoch(confVer, version uint64) *EntryBuilder {
	b.req.Header = &raft_cmdpb.RaftRequestHeader{
		RegionEpoch: &metapb.RegionEpoch{
			Version: version,
			ConfVer: confVer,
		},
	}
	return b
}

func (b *EntryBuilder) propose(applyCh chan<- *applyBatch, peerID, regionID uint64, callback *message.Callback) {
	prop := &proposal{
		isConfChange: false,
		index:        b.entry.Index,
		term:         b.entry.Term,
		cb:           callback,
	}
	msg := message.Msg{Type: message.MsgTypeApplyProposal, RegionID: regionID, Data: newRegionProposal(peerID, regionID, []*proposal{prop})}
	applyCh <- &applyBatch{
		msgs:  []message.Msg{msg},
		peers: make(map[uint64]*peerState),
	}
}

func (b *EntryBuilder) schedule(applyCh chan<- *applyBatch, peerID, regionID uint64, callback *message.Callback) {
	prop := &proposal{
		isConfChange: false,
		index:        b.entry.Index,
		term:         b.entry.Term,
		cb:           callback,
	}
	msg := message.Msg{Type: message.MsgTypeApplyProposal, RegionID: regionID, Data: newRegionProposal(peerID, regionID, []*proposal{prop})}
	applyCh <- &applyBatch{
		msgs:  []message.Msg{msg},
		peers: make(map[uint64]*peerState),
	}

	data, err := b.req.Marshal()
	if err != nil {
		panic("marshal err")
	}
	b.entry.Data = data
	apply := &apply{
		regionId: regionID,
		term:     b.entry.Term,
		entries:  []eraftpb.Entry{b.entry},
	}
	msg = message.Msg{Type: message.MsgTypeApply, RegionID: regionID, Data: apply}
	applyCh <- &applyBatch{
		msgs:  []message.Msg{msg},
		peers: make(map[uint64]*peerState),
	}
}

func TestHandleRaftCommittedEntries(t *testing.T) {
	engines := util.NewTestEngines()
	defer engines.Destroy()

	cfg := config.NewDefaultConfig()
	router, _ := CreateRaftBatchSystem(cfg)
	ctx := &GlobalContext{
		cfg:    cfg,
		engine: engines,
		router: router,
	}

	rw := newRaftWorker(ctx, router.workerSenders[0], router)
	reg := &registration{
		id: 3,
		region: &metapb.Region{
			Id: 1,
			Peers: []*metapb.Peer{{
				Id:      3,
				StoreId: 2,
			}},
			EndKey: []byte("k5"),
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 3,
			},
		},
	}
	newPeer := &peerState{
		msgCh:  router.workerSenders[0],
		closed: atomic.NewBool(false),
		apply:  newApplier(reg),
	}
	router.peers.Store(uint64(1), newPeer)

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go rw.runApply(wg)

	cb := message.NewCallback()
	NewEntryBuilder(1, 1).
		put(engine_util.CfDefault, []byte("k1"), []byte("v1")).
		put(engine_util.CfDefault, []byte("k2"), []byte("v2")).
		put(engine_util.CfDefault, []byte("k3"), []byte("v3")).
		epoch(1, 3).
		schedule(rw.applyCh, 3, 1, cb)
	resp := cb.WaitResp()
	require.True(t, resp.GetHeader().GetError() == nil)
	require.Equal(t, len(resp.GetResponses()), 3)
	fetchApplyRes(rw.raftCh)

	cb = message.NewCallback()
	NewEntryBuilder(2, 1).
		get(engine_util.CfDefault, []byte("k1")).
		get(engine_util.CfDefault, []byte("k2")).
		get(engine_util.CfDefault, []byte("k3")).
		epoch(1, 3).
		schedule(rw.applyCh, 3, 1, cb)
	resp = cb.WaitResp()
	require.True(t, resp.GetHeader().GetError() == nil)
	require.Equal(t, len(resp.GetResponses()), 3)
	require.True(t, bytes.Equal(resp.GetResponses()[0].GetGet().Value, []byte("v1")))
	require.True(t, bytes.Equal(resp.GetResponses()[1].GetGet().Value, []byte("v2")))
	require.True(t, bytes.Equal(resp.GetResponses()[2].GetGet().Value, []byte("v3")))

	applyRes := fetchApplyRes(rw.raftCh)
	require.Equal(t, applyRes.applyState.AppliedIndex, uint64(2))
	require.Equal(t, applyRes.appliedIndexTerm, uint64(1))

	cb = message.NewCallback()
	NewEntryBuilder(3, 2).
		put(engine_util.CfLock, []byte("k1"), []byte("v11")).
		delete(engine_util.CfDefault, []byte("k2")).
		epoch(1, 3).
		schedule(rw.applyCh, 3, 1, cb)
	resp = cb.WaitResp()
	require.True(t, resp.GetHeader().GetError() == nil)
	require.Equal(t, newPeer.apply.appliedIndexTerm, uint64(2))
	require.Equal(t, newPeer.apply.applyState.AppliedIndex, uint64(3))
	applyRes = fetchApplyRes(rw.raftCh)
	require.Equal(t, applyRes.regionID, uint64(1))
	require.Equal(t, applyRes.applyState.AppliedIndex, uint64(3))
	require.Equal(t, applyRes.appliedIndexTerm, uint64(2))
	require.Equal(t, len(applyRes.execResults), 0)

	cb = message.NewCallback()
	NewEntryBuilder(4, 2).
		snap().
		epoch(1, 3).
		schedule(rw.applyCh, 3, 1, cb)
	resp = cb.WaitResp()
	require.True(t, resp.GetHeader().GetError() == nil)
	require.Equal(t, len(resp.GetResponses()), 1)
	val, err := engine_util.GetCFFromTxn(cb.RegionSnap.Txn, engine_util.CfLock, []byte("k1"))
	require.Nil(t, err)
	require.True(t, bytes.Equal(val, []byte("v11")))
	applyRes = fetchApplyRes(rw.raftCh)
	require.Equal(t, applyRes.applyState.AppliedIndex, uint64(4))
	require.Equal(t, applyRes.appliedIndexTerm, uint64(2))

	cb = message.NewCallback()
	NewEntryBuilder(5, 2).
		put(engine_util.CfDefault, []byte("k2"), []byte("v2")).
		epoch(1, 1).
		schedule(rw.applyCh, 3, 1, cb)
	resp = cb.WaitResp()
	require.True(t, resp.GetHeader().GetError().GetEpochNotMatch() != nil)
	applyRes = fetchApplyRes(rw.raftCh)
	require.Equal(t, applyRes.applyState.AppliedIndex, uint64(5))
	require.Equal(t, applyRes.appliedIndexTerm, uint64(2))

	cb = message.NewCallback()
	NewEntryBuilder(6, 2).
		put(engine_util.CfDefault, []byte("k3"), []byte("v31")).
		put(engine_util.CfDefault, []byte("k5"), []byte("v5")).
		epoch(1, 3).
		schedule(rw.applyCh, 3, 1, cb)
	resp = cb.WaitResp()
	require.True(t, resp.GetHeader().GetError().GetKeyNotInRegion() != nil)
	applyRes = fetchApplyRes(rw.raftCh)
	require.Equal(t, applyRes.applyState.AppliedIndex, uint64(6))
	require.Equal(t, applyRes.appliedIndexTerm, uint64(2))
	val, err = engine_util.GetCF(engines.Kv, engine_util.CfDefault, []byte("k3"))
	require.Nil(t, err)
	// a write batch should be atomic
	require.True(t, bytes.Equal(val, []byte("v3")))

	cb1 := message.NewCallback()
	NewEntryBuilder(7, 2).
		propose(rw.applyCh, 3, 1, cb1)
	cb = message.NewCallback()
	NewEntryBuilder(7, 3).
		delete(engine_util.CfLock, []byte("k1")).
		delete(engine_util.CfWrite, []byte("k1")).
		epoch(1, 3).
		schedule(rw.applyCh, 3, 1, cb)
	resp1 := cb1.WaitResp()
	require.True(t, resp1.GetHeader().GetError().GetStaleCommand() != nil)
	resp = cb.WaitResp()
	require.True(t, resp.GetHeader().GetError() == nil)
	applyRes = fetchApplyRes(rw.raftCh)
	require.Equal(t, applyRes.applyState.AppliedIndex, uint64(7))
	require.Equal(t, applyRes.appliedIndexTerm, uint64(3))
}

func fetchApplyRes(raftCh <-chan message.Msg) *applyTaskRes {
	select {
	case msg := <-raftCh:
		if msg.Type != message.MsgTypeApplyRes {
			panic("unexpected apply res")
		}
		return msg.Data.(*applyTaskRes)
	case <-time.After(time.Second):
		panic("no apply res received")
	}
}

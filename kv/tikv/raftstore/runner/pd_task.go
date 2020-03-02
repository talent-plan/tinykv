package runner

import (
	"context"

	"github.com/Connor1996/badger"
	"github.com/ngaut/log"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/tikv/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/shirou/gopsutil/disk"
)

type PdAskBatchSplitTask struct {
	Region    *metapb.Region
	SplitKeys [][]byte
	Peer      *metapb.Peer
	Callback  *message.Callback
}

type PdRegionHeartbeatTask struct {
	Region          *metapb.Region
	Peer            *metapb.Peer
	DownPeers       []*pdpb.PeerStats
	PendingPeers    []*metapb.Peer
	ApproximateSize *uint64
}

type PdStoreHeartbeatTask struct {
	Stats  *pdpb.StoreStats
	Engine *badger.DB
	Path   string
}

type pdTaskHandler struct {
	storeID  uint64
	pdClient pd.Client
	router   message.RaftRouter
}

func NewPDTaskHandler(storeID uint64, pdClient pd.Client, router message.RaftRouter) *pdTaskHandler {
	return &pdTaskHandler{
		storeID:  storeID,
		pdClient: pdClient,
		router:   router,
	}
}

func (r *pdTaskHandler) Handle(t worker.Task) {
	switch t.Tp {
	case worker.TaskTypePDAskBatchSplit:
		r.onAskBatchSplit(t.Data.(*PdAskBatchSplitTask))
	case worker.TaskTypePDHeartbeat:
		r.onHeartbeat(t.Data.(*PdRegionHeartbeatTask))
	case worker.TaskTypePDStoreHeartbeat:
		r.onStoreHeartbeat(t.Data.(*PdStoreHeartbeatTask))
	default:
		log.Error("unsupported worker.Task type:", t.Tp)
	}
}

func (r *pdTaskHandler) Start() {
	r.pdClient.SetRegionHeartbeatResponseHandler(r.storeID, r.onRegionHeartbeatResponse)
}

func (r *pdTaskHandler) onRegionHeartbeatResponse(resp *pdpb.RegionHeartbeatResponse) {
	if changePeer := resp.GetChangePeer(); changePeer != nil {
		r.sendAdminRequest(resp.RegionId, resp.RegionEpoch, resp.TargetPeer, &raft_cmdpb.AdminRequest{
			CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerRequest{
				ChangeType: changePeer.ChangeType,
				Peer:       changePeer.Peer,
			},
		}, message.NewCallback())
	} else if transferLeader := resp.GetTransferLeader(); transferLeader != nil {
		r.sendAdminRequest(resp.RegionId, resp.RegionEpoch, resp.TargetPeer, &raft_cmdpb.AdminRequest{
			CmdType: raft_cmdpb.AdminCmdType_TransferLeader,
			TransferLeader: &raft_cmdpb.TransferLeaderRequest{
				Peer: transferLeader.Peer,
			},
		}, message.NewCallback())
	}
}

func (r *pdTaskHandler) onAskBatchSplit(t *PdAskBatchSplitTask) {
	resp, err := r.pdClient.AskBatchSplit(context.TODO(), t.Region, len(t.SplitKeys))
	if err != nil {
		log.Error(err)
		return
	}
	srs := make([]*raft_cmdpb.SplitRequest, len(resp.Ids))
	for i, splitID := range resp.Ids {
		srs[i] = &raft_cmdpb.SplitRequest{
			SplitKey:    t.SplitKeys[i],
			NewRegionId: splitID.NewRegionId,
			NewPeerIds:  splitID.NewPeerIds,
		}
	}
	aq := &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_BatchSplit,
		Splits: &raft_cmdpb.BatchSplitRequest{
			Requests: srs,
		},
	}
	r.sendAdminRequest(t.Region.GetId(), t.Region.GetRegionEpoch(), t.Peer, aq, t.Callback)
}

func (r *pdTaskHandler) onHeartbeat(t *PdRegionHeartbeatTask) {
	var size int64
	if t.ApproximateSize != nil {
		size = int64(*t.ApproximateSize)
	}

	req := &pdpb.RegionHeartbeatRequest{
		Region:          t.Region,
		Leader:          t.Peer,
		PendingPeers:    t.PendingPeers,
		ApproximateSize: uint64(size),
	}
	r.pdClient.RegionHeartbeat(req)
}

func (r *pdTaskHandler) onStoreHeartbeat(t *PdStoreHeartbeatTask) {
	diskStat, err := disk.Usage(t.Path)
	if err != nil {
		log.Error(err)
		return
	}

	capacity := diskStat.Total
	lsmSize, vlogSize := t.Engine.Size()
	usedSize := t.Stats.UsedSize + uint64(lsmSize) + uint64(vlogSize) // t.Stats.UsedSize contains size of snapshot files.
	available := uint64(0)
	if capacity > usedSize {
		available = capacity - usedSize
	}

	t.Stats.Capacity = capacity
	t.Stats.UsedSize = usedSize
	t.Stats.Available = available

	r.pdClient.StoreHeartbeat(context.TODO(), t.Stats)
}

func (r *pdTaskHandler) sendAdminRequest(regionID uint64, epoch *metapb.RegionEpoch, peer *metapb.Peer, req *raft_cmdpb.AdminRequest, callback *message.Callback) {
	cmd := &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId:    regionID,
			Peer:        peer,
			RegionEpoch: epoch,
		},
		AdminRequest: req,
	}
	r.router.SendRaftCommand(cmd, callback)
}

package test_raftstore

import (
	"bytes"
	"context"
	"sync"

	"github.com/google/btree"
	"github.com/ngaut/log"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	"github.com/pingcap/errors"
)

var _ btree.Item = &regionItem{}

type regionItem struct {
	region metapb.Region
}

// Less returns true if the region start key is less than the other.
func (r *regionItem) Less(other btree.Item) bool {
	left := r.region.GetStartKey()
	right := other.(*regionItem).region.GetStartKey()
	return bytes.Compare(left, right) < 0
}

func (r *regionItem) Contains(key []byte) bool {
	start, end := r.region.GetStartKey(), r.region.GetEndKey()
	return bytes.Compare(key, start) >= 0 && (len(end) == 0 || bytes.Compare(key, end) < 0)
}

type OperatorType int64

const (
	OperatorTypeAddPeer        = 1
	OperatorTypeRemovePeer     = 2
	OperatorTypeTransferLeader = 3
)

type Operator struct {
	Type OperatorType
	Data interface{}
}

type OpAddPeer struct {
	peer    metapb.Peer
	pending bool
}

type OpRemovePeer struct {
	peer metapb.Peer
}

type OpTransferLeader struct {
	peer metapb.Peer
}

type Store struct {
	store                    metapb.Store
	heartbeatResponseHandler func(*pdpb.RegionHeartbeatResponse)
}

func NewStore(store *metapb.Store) *Store {
	return &Store{
		store: *store,
		heartbeatResponseHandler: nil,
	}
}

type MockPDClient struct {
	sync.RWMutex

	clusterID uint64

	meta         metapb.Cluster
	stores       map[uint64]*Store
	regionsRange btree.BTree       // key -> region
	regionsKey   map[uint64][]byte // regionID -> startKey

	baseID uint64

	operators    map[uint64]*Operator
	leaders      map[uint64]*metapb.Peer // regionID -> peer
	pendingPeers map[uint64]*metapb.Peer // peerID -> peer

	bootstrapped bool
}

// Implement PDClient interface
func (m *MockPDClient) GetClusterID(ctx context.Context) uint64 {
	m.RLock()
	defer m.RUnlock()
	return m.clusterID
}

func (m *MockPDClient) AllocID(ctx context.Context) (uint64, error) {
	m.Lock()
	defer m.Unlock()
	ret := m.baseID
	m.baseID++
	return ret, nil
}

func (m *MockPDClient) Bootstrap(ctx context.Context, store *metapb.Store, region *metapb.Region) (*pdpb.BootstrapResponse, error) {
	m.Lock()
	defer m.Unlock()

	resp := &pdpb.BootstrapResponse{
		Header: &pdpb.ResponseHeader{ClusterId: m.clusterID},
	}

	if m.bootstrapped == true || len(m.regionsKey) != 0 {
		m.bootstrapped = true
		resp.Header.Error = &pdpb.Error{
			Type:    pdpb.ErrorType_NOT_BOOTSTRAPPED,
			Message: "cluster is not bootstrapped",
		}
		return resp, nil
	}

	m.stores[store.GetId()] = NewStore(store)
	m.addRegionLocked(region)
	m.bootstrapped = true
	return resp, nil
}

func (m *MockPDClient) IsBootstrapped(ctx context.Context) (bool, error) {
	m.RLock()
	defer m.RUnlock()
	return m.bootstrapped, nil
}

func (m *MockPDClient) checkBootstrap() error {
	if bootstrapped, _ := m.IsBootstrapped(context.TODO()); !bootstrapped {
		return errors.New("not bootstrapped")
	}
	return nil
}

func (m *MockPDClient) PutStore(ctx context.Context, store *metapb.Store) error {
	if err := m.checkBootstrap(); err != nil {
		return err
	}
	m.Lock()
	defer m.Unlock()

	s := NewStore(store)
	m.stores[store.GetId()] = s
	return nil
}

func (m *MockPDClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	if err := m.checkBootstrap(); err != nil {
		return nil, err
	}
	m.RLock()
	defer m.RUnlock()

	s, ok := m.stores[storeID]
	if !ok {
		return nil, errors.Errorf("store %d not found", storeID)
	}
	return &s.store, nil
}

func (m *MockPDClient) GetRegion(ctx context.Context, key []byte) (*metapb.Region, *metapb.Peer, error) {
	if err := m.checkBootstrap(); err != nil {
		return nil, nil, err
	}
	m.RLock()
	defer m.RUnlock()
	region, leader := m.getRegionLocked(key)
	return region, leader, nil
}

func (m *MockPDClient) getRegionLocked(key []byte) (*metapb.Region, *metapb.Peer) {
	result := m.findRegion(key)
	if result == nil {
		return nil, nil
	}

	leader := m.leaders[result.region.GetId()]
	return &result.region, leader
}

func (m *MockPDClient) GetRegionByID(ctx context.Context, regionID uint64) (*metapb.Region, *metapb.Peer, error) {
	if err := m.checkBootstrap(); err != nil {
		return nil, nil, err
	}
	m.RLock()
	defer m.RUnlock()
	return m.getRegionByIDLocked(regionID)
}

func (m *MockPDClient) getRegionByIDLocked(regionID uint64) (*metapb.Region, *metapb.Peer, error) {
	startKey := m.regionsKey[regionID]
	region, leader := m.getRegionLocked(startKey)
	return region, leader, nil
}

func (m *MockPDClient) AskBatchSplit(ctx context.Context, region *metapb.Region, count int) (*pdpb.AskBatchSplitResponse, error) {
	resp := new(pdpb.AskBatchSplitResponse)
	resp.Header = &pdpb.ResponseHeader{ClusterId: m.clusterID}
	curRegion, _, err := m.GetRegionByID(ctx, region.GetId())
	if err != nil {
		return resp, err
	}
	if util.IsEpochStale(region.RegionEpoch, curRegion.RegionEpoch) {
		return resp, errors.New("epoch is stale")
	}

	for i := 0; i < count; i++ {
		ids := new(pdpb.SplitID)
		id, _ := m.AllocID(ctx)
		ids.NewRegionId = id

		for range region.GetPeers() {
			id, _ := m.AllocID(ctx)
			ids.NewPeerIds = append(ids.NewPeerIds, id)
		}
		resp.Ids = append(resp.Ids, ids)
	}
	return resp, nil
}

func (m *MockPDClient) StoreHeartbeat(ctx context.Context, stats *pdpb.StoreStats) error {
	if err := m.checkBootstrap(); err != nil {
		return err
	}
	// nothing need to do
	return nil
}

func (m *MockPDClient) RegionHeartbeat(req *pdpb.RegionHeartbeatRequest) error {
	if err := m.checkBootstrap(); err != nil {
		return err
	}

	m.Lock()
	defer m.Unlock()

	regionID := req.Region.GetId()
	for _, p := range req.Region.GetPeers() {
		delete(m.pendingPeers, p.GetId())
	}
	for _, p := range req.GetPendingPeers() {
		m.pendingPeers[p.GetId()] = p
	}
	m.leaders[regionID] = req.Leader

	if err := m.handleHeartbeatVersion(req.Region); err != nil {
		return err
	}
	if err := m.handleHeartbeatConfVersion(req.Region); err != nil {
		return err
	}

	resp := &pdpb.RegionHeartbeatResponse{
		Header:      &pdpb.ResponseHeader{ClusterId: m.clusterID},
		RegionId:    regionID,
		RegionEpoch: req.Region.GetRegionEpoch(),
		TargetPeer:  req.Leader,
	}
	if op := m.operators[regionID]; op != nil {
		if m.tryFinished(op, req.Region, req.Leader) {
			delete(m.operators, regionID)
		} else {
			m.makeRegionHeartbeatResponse(op, resp)
		}
		log.Debugf("[region %d] schedule %v", regionID, op)
	}

	store := m.stores[req.Leader.GetStoreId()]
	store.heartbeatResponseHandler(resp)
	return nil
}

func (m *MockPDClient) handleHeartbeatVersion(region *metapb.Region) error {
	if bytes.Compare(region.GetStartKey(), region.GetEndKey()) > 0 {
		panic("start key > end key")
	}

	for {
		searchRegion, _ := m.getRegionLocked(region.GetStartKey())
		if searchRegion == nil {
			m.addRegionLocked(region)
			return nil
		} else {
			if bytes.Equal(searchRegion.GetStartKey(), region.GetStartKey()) &&
				bytes.Equal(searchRegion.GetEndKey(), region.GetEndKey()) {
				// the two regions' range are same, must check epoch
				if util.IsEpochStale(region.RegionEpoch, searchRegion.RegionEpoch) {
					return errors.New("epoch is stale")
				}
				if searchRegion.RegionEpoch.Version < region.RegionEpoch.Version {
					m.removeRegionLocked(searchRegion)
					m.addRegionLocked(region)
				}
				return nil
			}

			if bytes.Compare(searchRegion.GetStartKey(), region.GetEndKey()) > 0 {
				// No range covers [start, end) now, insert directly.
				m.addRegionLocked(region)
				return nil
			} else {
				// overlap, remove old, insert new.
				// E.g, 1 [a, c) -> 1 [a, b) + 2 [b, c), either new 1 or 2 reports, the region
				// is overlapped with origin [a, c).
				if region.GetRegionEpoch().GetVersion() <= searchRegion.GetRegionEpoch().GetVersion() {
					return errors.New("epoch is stale")
				}
				m.removeRegionLocked(searchRegion)
			}
		}
	}
}

func (m *MockPDClient) handleHeartbeatConfVersion(region *metapb.Region) error {
	searchRegion, _ := m.getRegionLocked(region.GetStartKey())
	if util.IsEpochStale(region.RegionEpoch, searchRegion.RegionEpoch) {
		return errors.New("epoch is stale")
	}

	regionPeerLen := len(region.GetPeers())
	searchRegionPeerLen := len(searchRegion.GetPeers())

	if region.RegionEpoch.ConfVer > searchRegion.RegionEpoch.ConfVer {
		// If ConfVer changed, TinyKV has added/removed one peer already.
		// So scheduler and TinyKV can't have same peer count and can only have
		// only one different peer.
		if searchRegionPeerLen > regionPeerLen {
			if regionPeerLen-searchRegionPeerLen != 1 {
				panic("should only one conf change")
			}
			if len(GetDiffPeers(searchRegion, region)) != 1 {
				panic("should only one different peer")
			}
			if len(GetDiffPeers(region, searchRegion)) != 0 {
				panic("should include all peers")
			}
		} else if searchRegionPeerLen < regionPeerLen {
			if regionPeerLen-searchRegionPeerLen != 1 {
				panic("should only one conf change")
			}
			if len(GetDiffPeers(region, searchRegion)) != 1 {
				panic("should only one different peer")
			}
			if len(GetDiffPeers(searchRegion, region)) != 0 {
				panic("should include all peers")
			}
		} else {
			MustSamePeers(searchRegion, region)
			if searchRegion.RegionEpoch.ConfVer+1 != region.RegionEpoch.ConfVer {
				panic("unmatched conf version")
			}
			if searchRegion.RegionEpoch.Version+1 != region.RegionEpoch.Version {
				panic("unmatched version")
			}
		}

		// update the region.
		if m.regionsRange.ReplaceOrInsert(&regionItem{region: *region}) == nil {
			panic("update inexistent region ")
		}
	} else {
		MustSamePeers(searchRegion, region)
	}
	return nil
}

func (m *MockPDClient) tryFinished(op *Operator, region *metapb.Region, leader *metapb.Peer) bool {
	switch op.Type {
	case OperatorTypeAddPeer:
		add := op.Data.(OpAddPeer)
		if !add.pending {
			for _, p := range region.GetPeers() {
				if add.peer.GetId() == p.GetId() {
					add.pending = true
				} else {
					// TinyKV rejects AddNode.
					return false
				}
			}
		} else {
			_, found := m.pendingPeers[add.peer.GetId()]
			return !found
		}
	case OperatorTypeRemovePeer:
		remove := op.Data.(OpRemovePeer)
		for _, p := range region.GetPeers() {
			if remove.peer.GetId() == p.GetId() {
				return false
			}
		}
		return true
	case OperatorTypeTransferLeader:
		transfer := op.Data.(OpTransferLeader)
		return leader.GetId() == transfer.peer.GetId()
	}
	panic("unreachable")
}

func (m *MockPDClient) makeRegionHeartbeatResponse(op *Operator, resp *pdpb.RegionHeartbeatResponse) {
	switch op.Type {
	case OperatorTypeAddPeer:
		add := op.Data.(OpAddPeer)
		if !add.pending {
			resp.ChangePeer = &pdpb.ChangePeer{
				ChangeType: eraftpb.ConfChangeType_AddNode,
				Peer:       &add.peer,
			}
		}
	case OperatorTypeRemovePeer:
		remove := op.Data.(OpRemovePeer)
		resp.ChangePeer = &pdpb.ChangePeer{
			ChangeType: eraftpb.ConfChangeType_RemoveNode,
			Peer:       &remove.peer,
		}
	case OperatorTypeTransferLeader:
		transfer := op.Data.(OpTransferLeader)
		resp.TransferLeader = &pdpb.TransferLeader{
			Peer: &transfer.peer,
		}
	}
}

func (m *MockPDClient) SetRegionHeartbeatResponseHandler(storeID uint64, h func(*pdpb.RegionHeartbeatResponse)) {
	if h == nil {
		h = func(*pdpb.RegionHeartbeatResponse) {}
	}
	m.Lock()
	defer m.Unlock()
	store := m.stores[storeID]
	store.heartbeatResponseHandler = h
}

func (m *MockPDClient) Close() {
	// do nothing
}

func (m *MockPDClient) findRegion(key []byte) *regionItem {
	item := &regionItem{region: metapb.Region{StartKey: key}}

	var result *regionItem
	m.regionsRange.DescendLessOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem)
		return false
	})

	if result == nil || !result.Contains(key) {
		return nil
	}

	return result
}

func (m *MockPDClient) addRegionLocked(region *metapb.Region) {
	m.regionsKey[region.GetId()] = region.GetStartKey()
	m.regionsRange.ReplaceOrInsert(&regionItem{region: *region})
}

func (m *MockPDClient) removeRegionLocked(region *metapb.Region) {
	delete(m.regionsKey, region.GetId())
	result := m.findRegion(region.GetStartKey())
	if result == nil || result.region.GetId() != region.GetId() {
		return
	}
	m.regionsRange.Delete(result)
}

// Extra API for tests
func (m *MockPDClient) AddPeer(regionID uint64, peer metapb.Peer) {
	m.scheduleOperator(regionID, &Operator{
		Type: OperatorTypeAddPeer,
		Data: &OpAddPeer{
			peer:    peer,
			pending: false,
		},
	})
}

func (m *MockPDClient) RemovePeer(regionID uint64, peer metapb.Peer) {
	m.scheduleOperator(regionID, &Operator{
		Type: OperatorTypeRemovePeer,
		Data: &OpRemovePeer{
			peer: peer,
		},
	})
}

func (m *MockPDClient) TransferLeader(regionID uint64, peer metapb.Peer) {
	m.scheduleOperator(regionID, &Operator{
		Type: OperatorTypeTransferLeader,
		Data: &OpTransferLeader{
			peer: peer,
		},
	})
}

func (m *MockPDClient) scheduleOperator(regionID uint64, op *Operator) {
	m.Lock()
	defer m.Unlock()
	m.operators[regionID] = op
}

// Utilities
func MustSamePeers(left *metapb.Region, right *metapb.Region) {
	if len(left.GetPeers()) != len(right.GetPeers()) {
		panic("unmatched peers length")
	}
	for _, p := range left.GetPeers() {
		found := false
		for _, p1 := range right.GetPeers() {
			if p.GetStoreId() == p1.GetStoreId() {
				found = true
				break
			}
		}
		if !found {
			panic("not found the peer")
		}
	}
}

func GetDiffPeers(left *metapb.Region, right *metapb.Region) []*metapb.Peer {
	peers := make([]*metapb.Peer, 1)
	for _, p := range left.GetPeers() {
		found := false
		for _, p1 := range right.GetPeers() {
			if p.GetStoreId() == p1.GetStoreId() {
				found = true
				break
			}
		}
		if !found {
			peers = append(peers, p)
		}
	}
	return peers
}

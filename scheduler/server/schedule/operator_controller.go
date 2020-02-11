// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedule

import (
	"container/heap"
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/juju/ratelimit"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/cache"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// The source of dispatched region.
const (
	DispatchFromHeartBeat     = "heartbeat"
	DispatchFromNotifierQueue = "active push"
	DispatchFromCreate        = "create"
)

var (
	historyKeepTime    = 5 * time.Minute
	slowNotifyInterval = 5 * time.Second
	fastNotifyInterval = 2 * time.Second
	// PushOperatorTickInterval is the interval try to push the operator.
	PushOperatorTickInterval = 500 * time.Millisecond
	// StoreBalanceBaseTime represents the base time of balance rate.
	StoreBalanceBaseTime float64 = 60
)

// HeartbeatStreams is an interface of async region heartbeat.
type HeartbeatStreams interface {
	SendMsg(region *core.RegionInfo, msg *pdpb.RegionHeartbeatResponse)
}

// OperatorController is used to limit the speed of scheduling.
type OperatorController struct {
	sync.RWMutex
	ctx       context.Context
	cluster   opt.Cluster
	operators map[uint64]*operator.Operator
	hbStreams HeartbeatStreams
	histories *list.List
	counts    map[operator.OpKind]uint64
	opRecords *OperatorRecords
	// TODO: Need to clean up the unused store ID.
	storesLimit     map[uint64]*ratelimit.Bucket
	opNotifierQueue operatorQueue
}

// NewOperatorController creates a OperatorController.
func NewOperatorController(ctx context.Context, cluster opt.Cluster, hbStreams HeartbeatStreams) *OperatorController {
	return &OperatorController{
		ctx:             ctx,
		cluster:         cluster,
		operators:       make(map[uint64]*operator.Operator),
		hbStreams:       hbStreams,
		histories:       list.New(),
		counts:          make(map[operator.OpKind]uint64),
		opRecords:       NewOperatorRecords(ctx),
		storesLimit:     make(map[uint64]*ratelimit.Bucket),
		opNotifierQueue: make(operatorQueue, 0),
	}
}

// Ctx returns a context which will be canceled once RaftCluster is stopped.
// For now, it is only used to control the lifetime of TTL cache in schedulers.
func (oc *OperatorController) Ctx() context.Context {
	return oc.ctx
}

// Dispatch is used to dispatch the operator of a region.
func (oc *OperatorController) Dispatch(region *core.RegionInfo, source string) {
	// Check existed operator.
	if op := oc.GetOperator(region.GetID()); op != nil {
		timeout := op.IsTimeout()
		if step := op.Check(region); step != nil && !timeout {
			// When the "source" is heartbeat, the region may have a newer
			// confver than the region that the operator holds. In this case,
			// the operator is stale, and will not be executed even we would
			// have sent it to TiKV servers. Here, we just cancel it.
			origin := op.RegionEpoch()
			latest := region.GetRegionEpoch()
			changes := latest.GetConfVer() - origin.GetConfVer()
			if source == DispatchFromHeartBeat &&
				changes > uint64(op.ConfVerChanged(region)) {

				if oc.RemoveOperator(op) {
					log.Info("stale operator", zap.Uint64("region-id", region.GetID()), zap.Duration("takes", op.RunningTime()),
						zap.Reflect("operator", op), zap.Uint64("diff", changes))
					oc.opRecords.Put(op, pdpb.OperatorStatus_CANCEL)
				}

				return
			}

			oc.SendScheduleCommand(region, step, source)
			return
		}
		if op.IsFinish() && oc.RemoveOperator(op) {
			log.Info("operator finish", zap.Uint64("region-id", region.GetID()), zap.Duration("takes", op.RunningTime()), zap.Reflect("operator", op))
			oc.pushHistory(op)
			oc.opRecords.Put(op, pdpb.OperatorStatus_SUCCESS)
		} else if timeout && oc.RemoveOperator(op) {
			log.Info("operator timeout", zap.Uint64("region-id", region.GetID()), zap.Duration("takes", op.RunningTime()), zap.Reflect("operator", op))
			oc.opRecords.Put(op, pdpb.OperatorStatus_TIMEOUT)
		}
	}
}

func (oc *OperatorController) getNextPushOperatorTime(step operator.OpStep, now time.Time) time.Time {
	nextTime := slowNotifyInterval
	switch step.(type) {
	case operator.TransferLeader:
		nextTime = fastNotifyInterval
	}
	return now.Add(nextTime)
}

// pollNeedDispatchRegion returns the region need to dispatch,
// "next" is true to indicate that it may exist in next attempt,
// and false is the end for the poll.
func (oc *OperatorController) pollNeedDispatchRegion() (r *core.RegionInfo, next bool) {
	oc.Lock()
	defer oc.Unlock()
	if oc.opNotifierQueue.Len() == 0 {
		return nil, false
	}
	item := heap.Pop(&oc.opNotifierQueue).(*operatorWithTime)
	regionID := item.op.RegionID()
	op, ok := oc.operators[regionID]
	if !ok || op == nil {
		return nil, true
	}
	r = oc.cluster.GetRegion(regionID)
	if r == nil {
		_ = oc.removeOperatorLocked(op)
		log.Debug("remove operator because region disappeared",
			zap.Uint64("region-id", op.RegionID()),
			zap.Stringer("operator", op))
		oc.opRecords.Put(op, pdpb.OperatorStatus_CANCEL)
		return nil, true
	}
	step := op.Check(r)
	if step == nil {
		return r, true
	}
	now := time.Now()
	if now.Before(item.time) {
		heap.Push(&oc.opNotifierQueue, item)
		return nil, false
	}

	// pushes with new notify time.
	item.time = oc.getNextPushOperatorTime(step, now)
	heap.Push(&oc.opNotifierQueue, item)
	return r, true
}

// PushOperators periodically pushes the unfinished operator to the executor(TiKV).
func (oc *OperatorController) PushOperators() {
	for {
		r, next := oc.pollNeedDispatchRegion()
		if !next {
			break
		}
		if r == nil {
			continue
		}

		oc.Dispatch(r, DispatchFromNotifierQueue)
	}
}

// AddOperator adds operators to the running operators.
func (oc *OperatorController) AddOperator(ops ...*operator.Operator) bool {
	oc.Lock()
	defer oc.Unlock()

	if oc.exceedStoreLimit(ops...) || !oc.checkAddOperator(ops...) {
		for _, op := range ops {
			oc.opRecords.Put(op, pdpb.OperatorStatus_CANCEL)
		}
		return false
	}
	for _, op := range ops {
		oc.addOperatorLocked(op)
	}
	return true
}

// checkAddOperator checks if the operator can be added.
// There are several situations that cannot be added:
// - There is no such region in the cluster
// - The epoch of the operator and the epoch of the corresponding region are no longer consistent.
// - The region already has a higher priority or same priority operator.
func (oc *OperatorController) checkAddOperator(ops ...*operator.Operator) bool {
	for _, op := range ops {
		region := oc.cluster.GetRegion(op.RegionID())
		if region == nil {
			log.Debug("region not found, cancel add operator", zap.Uint64("region-id", op.RegionID()))
			return false
		}
		if region.GetRegionEpoch().GetVersion() != op.RegionEpoch().GetVersion() || region.GetRegionEpoch().GetConfVer() != op.RegionEpoch().GetConfVer() {
			log.Debug("region epoch not match, cancel add operator", zap.Uint64("region-id", op.RegionID()), zap.Reflect("old", region.GetRegionEpoch()), zap.Reflect("new", op.RegionEpoch()))
			return false
		}
		if old := oc.operators[op.RegionID()]; old != nil && !isHigherPriorityOperator(op, old) {
			log.Debug("already have operator, cancel add operator", zap.Uint64("region-id", op.RegionID()), zap.Reflect("old", old))
			return false
		}
	}
	return true
}

func isHigherPriorityOperator(new, old *operator.Operator) bool {
	return new.GetPriorityLevel() > old.GetPriorityLevel()
}

func (oc *OperatorController) addOperatorLocked(op *operator.Operator) bool {
	regionID := op.RegionID()

	log.Info("add operator", zap.Uint64("region-id", regionID), zap.Reflect("operator", op))

	// If there is an old operator, replace it. The priority should be checked
	// already.
	if old, ok := oc.operators[regionID]; ok {
		_ = oc.removeOperatorLocked(old)
		log.Info("replace old operator", zap.Uint64("region-id", regionID), zap.Duration("takes", old.RunningTime()), zap.Reflect("operator", old))
		oc.opRecords.Put(old, pdpb.OperatorStatus_REPLACE)
	}

	oc.operators[regionID] = op
	op.SetStartTime(time.Now())
	opInfluence := NewTotalOpInfluence([]*operator.Operator{op}, oc.cluster)
	for storeID := range opInfluence.StoresInfluence {
		stepCost := opInfluence.GetStoreInfluence(storeID).StepCost
		if stepCost == 0 {
			continue
		}
		oc.storesLimit[storeID].Take(stepCost)
	}
	oc.updateCounts(oc.operators)

	var step operator.OpStep
	if region := oc.cluster.GetRegion(op.RegionID()); region != nil {
		if step = op.Check(region); step != nil {
			oc.SendScheduleCommand(region, step, DispatchFromCreate)
		}
	}

	heap.Push(&oc.opNotifierQueue, &operatorWithTime{op: op, time: oc.getNextPushOperatorTime(step, time.Now())})
	return true
}

// RemoveOperator removes a operator from the running operators.
func (oc *OperatorController) RemoveOperator(op *operator.Operator) (found bool) {
	oc.Lock()
	defer oc.Unlock()
	return oc.removeOperatorLocked(op)
}

// GetOperatorStatus gets the operator and its status with the specify id.
func (oc *OperatorController) GetOperatorStatus(id uint64) *OperatorWithStatus {
	oc.Lock()
	defer oc.Unlock()
	if op, ok := oc.operators[id]; ok {
		return &OperatorWithStatus{
			Op:     op,
			Status: pdpb.OperatorStatus_RUNNING,
		}
	}
	return oc.opRecords.Get(id)
}

func (oc *OperatorController) removeOperatorLocked(op *operator.Operator) bool {
	regionID := op.RegionID()
	if cur := oc.operators[regionID]; cur == op {
		delete(oc.operators, regionID)
		oc.updateCounts(oc.operators)
		return true
	}
	return false
}

// GetOperator gets a operator from the given region.
func (oc *OperatorController) GetOperator(regionID uint64) *operator.Operator {
	oc.RLock()
	defer oc.RUnlock()
	return oc.operators[regionID]
}

// GetOperators gets operators from the running operators.
func (oc *OperatorController) GetOperators() []*operator.Operator {
	oc.RLock()
	defer oc.RUnlock()

	operators := make([]*operator.Operator, 0, len(oc.operators))
	for _, op := range oc.operators {
		operators = append(operators, op)
	}

	return operators
}

// SendScheduleCommand sends a command to the region.
func (oc *OperatorController) SendScheduleCommand(region *core.RegionInfo, step operator.OpStep, source string) {
	log.Info("send schedule command", zap.Uint64("region-id", region.GetID()), zap.Stringer("step", step), zap.String("source", source))
	switch st := step.(type) {
	case operator.TransferLeader:
		cmd := &pdpb.RegionHeartbeatResponse{
			TransferLeader: &pdpb.TransferLeader{
				Peer: region.GetStorePeer(st.ToStore),
			},
		}
		oc.hbStreams.SendMsg(region, cmd)
	case operator.AddPeer:
		if region.GetStorePeer(st.ToStore) != nil {
			// The newly added peer is pending.
			return
		}
		cmd := &pdpb.RegionHeartbeatResponse{
			ChangePeer: &pdpb.ChangePeer{
				ChangeType: eraftpb.ConfChangeType_AddNode,
				Peer: &metapb.Peer{
					Id:      st.PeerID,
					StoreId: st.ToStore,
				},
			},
		}
		oc.hbStreams.SendMsg(region, cmd)
	case operator.RemovePeer:
		cmd := &pdpb.RegionHeartbeatResponse{
			ChangePeer: &pdpb.ChangePeer{
				ChangeType: eraftpb.ConfChangeType_RemoveNode,
				Peer:       region.GetStorePeer(st.FromStore),
			},
		}
		oc.hbStreams.SendMsg(region, cmd)
	default:
		log.Error("unknown operator step", zap.Reflect("step", step))
	}
}

func (oc *OperatorController) pushHistory(op *operator.Operator) {
	oc.Lock()
	defer oc.Unlock()
	for _, h := range op.History() {
		oc.histories.PushFront(h)
	}
}

// PruneHistory prunes a part of operators' history.
func (oc *OperatorController) PruneHistory() {
	oc.Lock()
	defer oc.Unlock()
	p := oc.histories.Back()
	for p != nil && time.Since(p.Value.(operator.OpHistory).FinishTime) > historyKeepTime {
		prev := p.Prev()
		oc.histories.Remove(p)
		p = prev
	}
}

// GetHistory gets operators' history.
func (oc *OperatorController) GetHistory(start time.Time) []operator.OpHistory {
	oc.RLock()
	defer oc.RUnlock()
	histories := make([]operator.OpHistory, 0, oc.histories.Len())
	for p := oc.histories.Front(); p != nil; p = p.Next() {
		history := p.Value.(operator.OpHistory)
		if history.FinishTime.Before(start) {
			break
		}
		histories = append(histories, history)
	}
	return histories
}

// updateCounts updates resource counts using current pending operators.
func (oc *OperatorController) updateCounts(operators map[uint64]*operator.Operator) {
	for k := range oc.counts {
		delete(oc.counts, k)
	}
	for _, op := range operators {
		oc.counts[op.Kind()]++
	}
}

// OperatorCount gets the count of operators filtered by mask.
func (oc *OperatorController) OperatorCount(mask operator.OpKind) uint64 {
	oc.RLock()
	defer oc.RUnlock()
	var total uint64
	for k, count := range oc.counts {
		if k&mask != 0 {
			total += count
		}
	}
	return total
}

// GetOpInfluence gets OpInfluence.
func (oc *OperatorController) GetOpInfluence(cluster opt.Cluster) operator.OpInfluence {
	oc.RLock()
	defer oc.RUnlock()

	var res []*operator.Operator
	for _, op := range oc.operators {
		if !op.IsTimeout() && !op.IsFinish() {
			region := cluster.GetRegion(op.RegionID())
			if region != nil {
				res = append(res, op)
			}
		}
	}
	return NewUnfinishedOpInfluence(res, cluster)
}

// NewTotalOpInfluence creates a OpInfluence.
func NewTotalOpInfluence(operators []*operator.Operator, cluster opt.Cluster) operator.OpInfluence {
	influence := operator.OpInfluence{
		StoresInfluence: make(map[uint64]*operator.StoreInfluence),
	}

	for _, op := range operators {
		region := cluster.GetRegion(op.RegionID())
		if region != nil {
			op.TotalInfluence(influence, region)
		}
	}

	return influence
}

// NewUnfinishedOpInfluence creates a OpInfluence.
func NewUnfinishedOpInfluence(operators []*operator.Operator, cluster opt.Cluster) operator.OpInfluence {
	influence := operator.OpInfluence{
		StoresInfluence: make(map[uint64]*operator.StoreInfluence),
	}

	for _, op := range operators {
		if !op.IsTimeout() && !op.IsFinish() {
			region := cluster.GetRegion(op.RegionID())
			if region != nil {
				op.UnfinishedInfluence(influence, region)
			}
		}
	}

	return influence
}

// SetOperator is only used for test.
func (oc *OperatorController) SetOperator(op *operator.Operator) {
	oc.Lock()
	defer oc.Unlock()
	oc.operators[op.RegionID()] = op
}

// OperatorWithStatus records the operator and its status.
type OperatorWithStatus struct {
	Op     *operator.Operator
	Status pdpb.OperatorStatus
}

// MarshalJSON returns the status of operator as a JSON string
func (o *OperatorWithStatus) MarshalJSON() ([]byte, error) {
	return []byte(`"` + fmt.Sprintf("status: %s, operator: %s", o.Status.String(), o.Op.String()) + `"`), nil
}

// OperatorRecords remains the operator and its status for a while.
type OperatorRecords struct {
	ttl *cache.TTL
}

const operatorStatusRemainTime = 10 * time.Minute

// NewOperatorRecords returns a OperatorRecords.
func NewOperatorRecords(ctx context.Context) *OperatorRecords {
	return &OperatorRecords{
		ttl: cache.NewTTL(ctx, time.Minute, operatorStatusRemainTime),
	}
}

// Get gets the operator and its status.
func (o *OperatorRecords) Get(id uint64) *OperatorWithStatus {
	v, exist := o.ttl.Get(id)
	if !exist {
		return nil
	}
	return v.(*OperatorWithStatus)
}

// Put puts the operator and its status.
func (o *OperatorRecords) Put(op *operator.Operator, status pdpb.OperatorStatus) {
	id := op.RegionID()
	record := &OperatorWithStatus{
		Op:     op,
		Status: status,
	}
	o.ttl.Put(id, record)
}

// exceedStoreLimit returns true if the store exceeds the cost limit after adding the operator. Otherwise, returns false.
func (oc *OperatorController) exceedStoreLimit(ops ...*operator.Operator) bool {
	opInfluence := NewTotalOpInfluence(ops, oc.cluster)
	for storeID := range opInfluence.StoresInfluence {
		stepCost := opInfluence.GetStoreInfluence(storeID).StepCost
		if stepCost == 0 {
			continue
		}

		available := oc.getOrCreateStoreLimit(storeID).Available()
		if available < stepCost {
			return true
		}
	}
	return false
}

// SetAllStoresLimit is used to set limit of all stores.
func (oc *OperatorController) SetAllStoresLimit(rate float64) {
	oc.Lock()
	defer oc.Unlock()
	stores := oc.cluster.GetStores()
	for _, s := range stores {
		oc.newStoreLimit(s.GetID(), rate)
	}
}

// SetStoreLimit is used to set the limit of a store.
func (oc *OperatorController) SetStoreLimit(storeID uint64, rate float64) {
	oc.Lock()
	defer oc.Unlock()
	oc.newStoreLimit(storeID, rate)
}

// newStoreLimit is used to create the limit of a store.
func (oc *OperatorController) newStoreLimit(storeID uint64, rate float64) {
	capacity := operator.RegionInfluence
	if rate > 1 {
		capacity = int64(rate * float64(operator.RegionInfluence))
	}
	rate *= float64(operator.RegionInfluence)
	oc.storesLimit[storeID] = ratelimit.NewBucketWithRate(rate, capacity)
}

// getOrCreateStoreLimit is used to get or create the limit of a store.
func (oc *OperatorController) getOrCreateStoreLimit(storeID uint64) *ratelimit.Bucket {
	if oc.storesLimit[storeID] == nil {
		rate := oc.cluster.GetStoreBalanceRate() / StoreBalanceBaseTime
		oc.newStoreLimit(storeID, rate)
		oc.cluster.AttachAvailableFunc(storeID, func() bool {
			oc.RLock()
			defer oc.RUnlock()
			return oc.storesLimit[storeID].Available() >= operator.RegionInfluence
		})
	}
	return oc.storesLimit[storeID]
}

// GetAllStoresLimit is used to get limit of all stores.
func (oc *OperatorController) GetAllStoresLimit() map[uint64]float64 {
	oc.RLock()
	defer oc.RUnlock()
	ret := make(map[uint64]float64)
	for storeID, limit := range oc.storesLimit {
		store := oc.cluster.GetStore(storeID)
		if !store.IsTombstone() {
			ret[storeID] = limit.Rate() / float64(operator.RegionInfluence)
		}
	}
	return ret
}

// GetLeaderScheduleStrategy is to get leader schedule strategy
func (oc *OperatorController) GetLeaderScheduleStrategy() core.ScheduleStrategy {
	if oc.cluster == nil {
		return core.ByCount
	}
	return oc.cluster.GetLeaderScheduleStrategy()
}

// RemoveStoreLimit removes the store limit for a given store ID.
func (oc *OperatorController) RemoveStoreLimit(storeID uint64) {
	oc.Lock()
	defer oc.Unlock()
	delete(oc.storesLimit, storeID)
}

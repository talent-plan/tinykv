package raftstore

import (
	"bytes"
	"fmt"
	"time"

	"github.com/coocood/badger"
	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/tikv/dbreader"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/pingcap/tidb/util/codec"
	"github.com/uber-go/atomic"
)

const (
	WriteBatchMaxKeys  = 128
	DefaultApplyWBSize = 4 * 1024

	WriteTypeFlagPut      = 'P'
	WriteTypeFlagDelete   = 'D'
	WriteTypeFlagLock     = 'L'
	WriteTypeFlagRollback = 'R'
)

type pendingCmd struct {
	index uint64
	term  uint64
	cb    *Callback
}

type pendingCmdQueue struct {
	normals    []pendingCmd
	confChange *pendingCmd
}

func (q *pendingCmdQueue) popNormal(term uint64) *pendingCmd {
	if len(q.normals) == 0 {
		return nil
	}
	cmd := &q.normals[0]
	if cmd.term > term {
		return nil
	}
	q.normals = q.normals[1:]
	return cmd
}

func (q *pendingCmdQueue) appendNormal(cmd pendingCmd) {
	q.normals = append(q.normals, cmd)
}

func (q *pendingCmdQueue) takeConfChange() *pendingCmd {
	// conf change will not be affected when changing between follower and leader,
	// so there is no need to check term.
	cmd := q.confChange
	q.confChange = nil
	return cmd
}

// TODO: seems we don't need to separate conf change from normal entries.
func (q *pendingCmdQueue) setConfChange(cmd *pendingCmd) {
	q.confChange = cmd
}

type changePeer struct {
	confChange *eraftpb.ConfChange
	peer       *metapb.Peer
	region     *metapb.Region
}

type keyRange struct {
	startKey []byte
	endKey   []byte
}

type apply struct {
	regionId uint64
	term     uint64
	entries  []eraftpb.Entry
}

type applyMetrics struct {
	sizeDiffHint   uint64
	deleteKeysHint uint64
	writtenBytes   uint64
	writtenKeys    uint64
}

type applyTaskRes struct {
	regionID         uint64
	applyState       applyState
	appliedIndexTerm uint64
	execResults      []execResult
	metrics          applyMetrics
	merged           bool

	destroyPeerID uint64
}

type execResultChangePeer struct {
	cp changePeer
}

type execResultCompactLog struct {
	truncatedIndex uint64
	firstIndex     uint64
}

type execResultSplitRegion struct {
	regions []*metapb.Region
	derived *metapb.Region
}

type execResultPrepareMerge struct {
	region *metapb.Region
	state  *rspb.MergeState
}

type execResultCommitMerge struct {
	region *metapb.Region
	source *metapb.Region
}

type execResultRollbackMerge struct {
	region *metapb.Region
	commit uint64
}

type execResultComputeHash struct {
	region *metapb.Region
	index  uint64
	snap   *DBSnapshot
}

type execResultVerifyHash struct {
	index uint64
	hash  []byte
}

type execResultDeleteRange struct {
	ranges []keyRange
}

type execResult = interface{}

type applyResultType int

const (
	applyResultTypeNone              applyResultType = 0
	applyResultTypeExecResult        applyResultType = 1
	applyResultTypeWaitMergeResource applyResultType = 2
)

type applyResult struct {
	tp   applyResultType
	data interface{}
}

type applyExecContext struct {
	index      uint64
	term       uint64
	applyState applyState
}

type applyCallback struct {
	region *metapb.Region
	cbs    []*Callback
}

func (c *applyCallback) invokeAll(host *CoprocessorHost) {
	for _, cb := range c.cbs {
		if cb != nil {
			host.postApply(c.region, cb.resp)
			cb.wg.Done()
		}
	}
}

func (c *applyCallback) push(cb *Callback, resp *raft_cmdpb.RaftCmdResponse) {
	c.cbs = append(c.cbs, cb)
}

type callBackResponsePair struct {
	callBack *Callback
	response *raft_cmdpb.RaftCmdResponse
}

type proposal struct {
	isConfChange bool
	index        uint64
	term         uint64
	cb           *Callback
}

type regionProposal struct {
	Id       uint64
	RegionId uint64
	Props    []*proposal
}

func newRegionProposal(id uint64, regionId uint64, props []*proposal) *regionProposal {
	return &regionProposal{
		Id:       id,
		RegionId: regionId,
		Props:    props,
	}
}

type registration struct {
	id               uint64
	term             uint64
	applyState       applyState
	appliedIndexTerm uint64
	region           *metapb.Region
}

func newRegistration(peer *Peer) *registration {
	return &registration{
		id:               peer.PeerId(),
		term:             peer.Term(),
		applyState:       peer.Store().applyState,
		appliedIndexTerm: peer.Store().appliedIndexTerm,
		region:           peer.Region(),
	}
}

type GenSnapTask struct {
	regionID     uint64
	snapNotifier chan *eraftpb.Snapshot
}

func newGenSnapTask(regionID uint64, notifier chan *eraftpb.Snapshot) *GenSnapTask {
	return &GenSnapTask{
		regionID:     regionID,
		snapNotifier: notifier,
	}
}

func (t *GenSnapTask) generateAndScheduleSnapshot(regionSched chan<- task) {
	regionSched <- task{
		tp: taskTypeRegionGen,
		data: &regionTask{
			regionId: t.regionID,
			notifier: t.snapNotifier,
		},
	}
}

type applyRouter struct {
	router
}

func (r *applyRouter) scheduleTask(regionID uint64, msg Msg) {
	if err := r.send(regionID, msg); err == nil {
		return
	}
	switch msg.Type {
	case MsgTypeApplyRegistration:
		// Messages in one region are sent in sequence, so there is no race here.
		// However, this can't be handled inside control fsm, as messages can be
		// queued inside both queue of control fsm and normal fsm, which can reorder
		// messages.
		sender, applyFsm := newApplyFsmFromRegistration(msg.Data.(*registration))
		mb := newMailbox(sender, applyFsm)
		r.register(regionID, mb)
	case MsgTypeApplyProposal:
		log.Infof("target region %d is not found, drop proposals", regionID)
		props := msg.Data.(*regionProposal)
		for _, prop := range props.Props {
			cmd := pendingCmd{index: prop.index, term: prop.term, cb: prop.cb}
			notifyRegionRemoved(regionID, props.Id, cmd)
		}
	case MsgTypeApplyCatchUpLogs:
		cuLog := msg.Data.(*catchUpLogs)
		panic(fmt.Sprintf("region %d is removed before merged, failed to schedule %s", regionID, cuLog.merge))
	default:
		log.Infof("target region %d is not found, drop messages", regionID)
	}
}

type notifier struct {
	router *router
	sender chan<- Msg
}

func (n *notifier) notify(regionID uint64, msg Msg) {
	if n.router != nil {
		_ = n.router.send(regionID, msg)
	} else {
		n.sender <- msg
	}
}

type applyContext struct {
	tag              string
	timer            *time.Time
	host             *CoprocessorHost
	regionScheduler  chan<- task
	router           *applyRouter
	notifier         notifier
	engines          *Engines
	txn              *badger.Txn
	cbs              []applyCallback
	applyTaskResList []*applyTaskRes
	execCtx          *applyExecContext
	wb               *WriteBatch
	wbLastBytes      uint64
	wbLastKeys       uint64
	lastAppliedIndex uint64
	committedCount   int

	// Indicates that WAL can be synchronized when data is written to KV engine.
	enableSyncLog bool
	// Whether synchronize WAL is preferred.
	syncLogHint bool
	// Whether to use the delete range API instead of deleting one by one.
	useDeleteRange bool
}

func newApplyContext(tag string, host *CoprocessorHost, regionScheduler chan<- task, engines *Engines, router *applyRouter,
	notifier notifier, cfg *Config) *applyContext {
	return &applyContext{
		tag:             tag,
		host:            host,
		regionScheduler: regionScheduler,
		engines:         engines,
		router:          router,
		notifier:        notifier,
		enableSyncLog:   cfg.SyncLog,
		useDeleteRange:  cfg.UseDeleteRange,
		wb:              new(WriteBatch),
	}
}

/// Prepares for applying entries for `delegate`.
///
/// A general apply progress for a delegate is:
/// `prepare_for` -> `commit` [-> `commit` ...] -> `finish_for`.
/// After all delegates are handled, `write_to_db` method should be called.
func (ac *applyContext) prepareFor(d *applyDelegate) {
	if ac.wb == nil {
		ac.wb = new(WriteBatch)
		ac.wbLastBytes = 0
		ac.wbLastKeys = 0
	}
	ac.cbs = append(ac.cbs, applyCallback{region: d.region})
	ac.lastAppliedIndex = d.applyState.appliedIndex
}

/// Commits all changes have done for delegate. `persistent` indicates whether
/// write the changes into rocksdb.
///
/// This call is valid only when it's between a `prepare_for` and `finish_for`.
func (ac *applyContext) commit(d *applyDelegate) {
	if ac.lastAppliedIndex < d.applyState.appliedIndex {
		d.writeApplyState(ac.wb)
	}
	// last_applied_index doesn't need to be updated, set persistent to true will
	// force it call `prepare_for` automatically.
	ac.commitOpt(d, true)
}

func (ac *applyContext) commitOpt(d *applyDelegate, persistent bool) {
	d.updateMetrics(ac)
	if persistent {
		ac.writeToDB()
		ac.prepareFor(d)
	}
	ac.wbLastBytes = uint64(ac.wb.size)
	ac.wbLastKeys = uint64(len(ac.wb.entries))
}

/// Writes all the changes into badger.
func (ac *applyContext) writeToDB() {
	if ac.wb.size != 0 {
		if err := ac.wb.WriteToKV(ac.engines.kv); err != nil {
			panic(err)
		}
		ac.wb.Reset()
	}
	for _, cb := range ac.cbs {
		cb.invokeAll(ac.host)
	}
	ac.cbs = ac.cbs[:0]
}

/// Finishes `Apply`s for the delegate.
func (ac *applyContext) finishFor(d *applyDelegate, results []execResult) {
	if !d.pendingRemove {
		d.writeApplyState(ac.wb)
	}
	ac.commitOpt(d, false)
	res := &applyTaskRes{
		regionID:         d.region.Id,
		applyState:       d.applyState,
		execResults:      results,
		metrics:          d.metrics,
		appliedIndexTerm: d.appliedIndexTerm,
	}
	ac.applyTaskResList = append(ac.applyTaskResList, res)
}

func (ac *applyContext) deltaBytes() uint64 {
	return uint64(ac.wb.size) - ac.wbLastBytes
}

func (ac *applyContext) deltaKeys() uint64 {
	return uint64(len(ac.wb.entries)) - ac.wbLastKeys
}

func (ac *applyContext) getTxn() *badger.Txn {
	if ac.txn == nil {
		ac.txn = ac.engines.kv.db.NewTransaction(false)
	}
	return ac.txn
}

func (ac *applyContext) flush() {
	// TODO: this check is too hacky, need to be more verbose and less buggy.
	t := ac.timer
	ac.timer = nil
	if t == nil {
		return
	}
	if ac.txn != nil {
		ac.txn.Discard()
		ac.txn = nil
	}
	// Write to engine
	// raftsotre.sync-log = true means we need prevent data loss when power failure.
	// take raft log gc for example, we write kv WAL first, then write raft WAL,
	// if power failure happen, raft WAL may synced to disk, but kv WAL may not.
	// so we use sync-log flag here.
	ac.writeToDB()
	if len(ac.applyTaskResList) > 0 {
		for _, res := range ac.applyTaskResList {
			ac.notifier.notify(res.regionID, NewPeerMsg(MsgTypeApplyRes, res.regionID, res))
		}
		ac.applyTaskResList = ac.applyTaskResList[:0]
	}
	ac.committedCount = 0
}

/// Calls the callback of `cmd` when the Region is removed.
func notifyRegionRemoved(regionID, peerID uint64, cmd pendingCmd) {
	log.Debugf("region %d is removed, peerID %d, index %d, term %d", regionID, peerID, cmd.index, cmd.term)
	notifyReqRegionRemoved(regionID, cmd.cb)
}

func notifyReqRegionRemoved(regionID uint64, cb *Callback) {
	cb.Done(ErrRespRegionNotFound(regionID))
}

/// Calls the callback of `cmd` when it can not be processed further.
func notifyStaleCommand(regionID, peerID, term uint64, cmd pendingCmd) {
	log.Infof("command is stale, skip. regionID %d, peerID %d, index %d, term %d",
		regionID, peerID, cmd.index, cmd.term)
	notifyStaleReq(term, cmd.cb)
}

func notifyStaleReq(term uint64, cb *Callback) {
	cb.Done(ErrRespStaleCommand(term))
}

/// Checks if a write is needed to be issued before handling the command.
func shouldWriteToEngine(cmd *raft_cmdpb.RaftCmdRequest, wbKeys int) bool {
	if cmd.AdminRequest != nil {
		switch cmd.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_ComputeHash, // ComputeHash require an up to date snapshot.
			raft_cmdpb.AdminCmdType_CommitMerge, // Merge needs to get the latest apply index.
			raft_cmdpb.AdminCmdType_RollbackMerge:
			return true
		}
	}

	// When write batch contains more than `recommended` keys, write the batch
	// to engine.
	if wbKeys >= WriteBatchMaxKeys {
		return true
	}

	// Some commands may modify keys covered by the current write batch, so we
	// must write the current write batch to the engine first.
	for _, req := range cmd.Requests {
		if req.DeleteRange != nil {
			return true
		}
		if req.IngestSst != nil {
			return true
		}
	}
	return false
}

/// A struct that stores the state related to Merge.
///
/// When executing a `CommitMerge`, the source peer may have not applied
/// to the required index, so the target peer has to abort current execution
/// and wait for it asynchronously.
///
/// When rolling the stack, all states required to recover are stored in
/// this struct.
/// TODO: check whether generator/coroutine is a good choice in this case.
type waitSourceMergeState struct {
	/// All of the entries that need to continue to be applied after
	/// the source peer has applied its logs.
	pendingEntries []eraftpb.Entry
	/// All of messages that need to continue to be handled after
	/// the source peer has applied its logs and pending entries
	/// are all handled.
	pendingMsgs []Msg
	/// A flag that indicates whether the source peer has applied to the required
	/// index. If the source peer is ready, this flag should be set to the region id
	/// of source peer.
	readyToMerge *atomic.Uint64
	/// When handling `CatchUpLogs` message, maybe there is a merge cascade, namely,
	/// a source peer to catch up logs whereas the logs contain a `CommitMerge`.
	/// In this case, the source peer needs to merge another source peer first, so storing the
	/// `CatchUpLogs` message in this field, and once the cascaded merge and all other pending
	/// msgs are handled, the source peer will check this field and then send `LogsUpToDate`
	/// message to its target peer.
	catchUpLogs *catchUpLogs
}

func (s *waitSourceMergeState) String() string {
	return fmt.Sprintf("waitSourceMergeState{pending_entries:%d, pending_msgs:%d, ready_to_merge:%d, catch_up_logs:%v}",
		len(s.pendingEntries), len(s.pendingMsgs), s.readyToMerge.Load(), s.catchUpLogs != nil)
}

/// The apply delegate of a Region which is responsible for handling committed
/// raft log entries of a Region.
///
/// `Apply` is a term of Raft, which means executing the actual commands.
/// In Raft, once some log entries are committed, for every peer of the Raft
/// group will apply the logs one by one. For write commands, it does write or
/// delete to local engine; for admin commands, it does some meta change of the
/// Raft group.
///
/// `Delegate` is just a structure to congregate all apply related fields of a
/// Region. The apply worker receives all the apply tasks of different Regions
/// located at this store, and it will get the corresponding apply delegate to
/// handle the apply task to make the code logic more clear.
type applyDelegate struct {
	id     uint64
	term   uint64
	region *metapb.Region
	tag    string

	/// If the delegate should be stopped from polling.
	/// A delegate can be stopped in conf change, merge or requested by destroy message.
	stopped bool
	/// Set to true when removing itself because of `ConfChangeType::RemoveNode`, and then
	/// any following committed logs in same Ready should be applied failed.
	pendingRemove bool

	/// The commands waiting to be committed and applied
	pendingCmds pendingCmdQueue

	/// Marks the delegate as merged by CommitMerge.
	merged bool

	/// Indicates the peer is in merging, if that compact log won't be performed.
	isMerging bool
	/// Records the epoch version after the last merge.
	lastMergeVersion uint64
	/// A temporary state that keeps track of the progress of the source peer state when
	/// CommitMerge is unable to be executed.
	waitMergeState *waitSourceMergeState
	// ID of last region that reports ready.
	readySourceRegion uint64

	/// We writes apply_state to KV DB, in one write batch together with kv data.
	///
	/// If we write it to Raft DB, apply_state and kv data (Put, Delete) are in
	/// separate WAL file. When power failure, for current raft log, apply_index may synced
	/// to file, but KV data may not synced to file, so we will lose data.
	applyState applyState
	/// The term of the raft log at applied index.
	appliedIndexTerm uint64

	/// The local metrics, and it will be flushed periodically.
	metrics applyMetrics
}

func newApplyDelegate(reg *registration) *applyDelegate {
	return &applyDelegate{
		id:               reg.id,
		tag:              fmt.Sprintf("[region %d] %d", reg.region.Id, reg.id),
		region:           reg.region,
		applyState:       reg.applyState,
		appliedIndexTerm: reg.appliedIndexTerm,
		term:             reg.term,
	}
}

/// Handles all the committed_entries, namely, applies the committed entries.
func (d *applyDelegate) handleRaftCommittedEntries(aCtx *applyContext, committedEntries []eraftpb.Entry) {
	if len(committedEntries) == 0 {
		return
	}
	aCtx.prepareFor(d)
	aCtx.committedCount += len(committedEntries)
	// If we send multiple ConfChange commands, only first one will be proposed correctly,
	// others will be saved as a normal entry with no data, so we must re-propose these
	// commands again.
	aCtx.committedCount += len(committedEntries)
	var results []execResult
	for i := range committedEntries {
		entry := &committedEntries[i]
		if d.pendingRemove {
			// This peer is about to be destroyed, skip everything.
			break
		}
		expectedIndex := d.applyState.appliedIndex + 1
		if expectedIndex != entry.Index {
			// Msg::CatchUpLogs may have arrived before Msg::Apply.
			if expectedIndex > entry.GetIndex() && d.isMerging {
				log.Infof("skip log as it's already applied. region_id %d, peer_id %d, index %d",
					d.region.Id, d.id, entry.Index)
				continue
			}
			panic(fmt.Sprintf("%s expect index %d, but got %d", d.tag, expectedIndex, entry.Index))
		}
		var res applyResult
		switch entry.EntryType {
		case eraftpb.EntryType_EntryNormal:
			res = d.handleRaftEntryNormal(aCtx, entry)
		case eraftpb.EntryType_EntryConfChange:
			res = d.handleRaftEntryConfChange(aCtx, entry)
		}
		switch res.tp {
		case applyResultTypeNone:
		case applyResultTypeExecResult:
			results = append(results, res.data)
		case applyResultTypeWaitMergeResource:
			readyToMerge := res.data.(*atomic.Uint64)
			aCtx.committedCount -= len(committedEntries) - i
			pendingEntries := make([]eraftpb.Entry, 0, len(committedEntries)-i)
			// Note that CommitMerge is skipped when `WaitMergeSource` is returned.
			// So we need to enqueue it again and execute it again when resuming.
			pendingEntries = append(pendingEntries, committedEntries[i:]...)
			aCtx.finishFor(d, results)
			d.waitMergeState = &waitSourceMergeState{
				pendingEntries: pendingEntries,
				readyToMerge:   readyToMerge,
			}
			return
		}
	}
	aCtx.finishFor(d, results)
}

func (d *applyDelegate) updateMetrics(aCtx *applyContext) {
	d.metrics.writtenBytes += aCtx.deltaBytes()
	d.metrics.writtenKeys += aCtx.deltaKeys()
}

func (d *applyDelegate) writeApplyState(wb *WriteBatch) {
	wb.Set(ApplyStateKey(d.region.Id), d.applyState.Marshal())
}

func (d *applyDelegate) handleRaftEntryNormal(aCtx *applyContext, entry *eraftpb.Entry) applyResult {
	index := entry.Index
	term := entry.Term
	if len(entry.Data) > 0 {
		cmd := new(raft_cmdpb.RaftCmdRequest)
		err := cmd.Unmarshal(entry.Data)
		if err != nil {
			panic(err)
		}
		if shouldWriteToEngine(cmd, len(aCtx.wb.entries)) {
			aCtx.commit(d)
		}
		return d.processRaftCmd(aCtx, index, term, cmd)
	}

	// when a peer become leader, it will send an empty entry.
	d.applyState.appliedIndex = index
	d.appliedIndexTerm = term
	y.Assert(term > 0)
	for {
		cmd := d.pendingCmds.popNormal(term - 1)
		if cmd == nil {
			break
		}
		// apparently, all the callbacks whose term is less than entry's term are stale.
		cb := &aCtx.cbs[len(aCtx.cbs)-1]
		cmd.cb.resp = ErrRespStaleCommand(term)
		cb.cbs = append(cb.cbs, cmd.cb)
	}
	return applyResult{}
}

func (d *applyDelegate) handleRaftEntryConfChange(aCtx *applyContext, entry *eraftpb.Entry) applyResult {
	index := entry.Index
	term := entry.Term
	confChange := new(eraftpb.ConfChange)
	if err := confChange.Unmarshal(entry.Data); err != nil {
		panic(err)
	}
	cmd := new(raft_cmdpb.RaftCmdRequest)
	if err := cmd.Unmarshal(confChange.Context); err != nil {
		panic(err)
	}
	result := d.processRaftCmd(aCtx, index, term, cmd)
	switch result.tp {
	case applyResultTypeNone:
		// If failed, tell Raft that the `ConfChange` was aborted.
		return applyResult{tp: applyResultTypeExecResult, data: &execResultChangePeer{}}
	case applyResultTypeExecResult:
		cp := result.data.(*execResultChangePeer)
		cp.cp.confChange = confChange
		return applyResult{tp: applyResultTypeExecResult, data: result.data}
	default:
		panic("unreachable")
	}
}

func (d *applyDelegate) findCallback(index, term uint64, isConfChange bool) *Callback {
	regionID := d.region.Id
	peerID := d.id
	if isConfChange {
		cmd := d.pendingCmds.takeConfChange()
		if cmd == nil {
			return nil
		}
		if cmd.index == index && cmd.term == term {
			return cmd.cb
		}
		notifyStaleCommand(regionID, peerID, term, *cmd)
		return nil
	}
	for {
		head := d.pendingCmds.popNormal(term)
		if head == nil {
			break
		}
		if head.index == index && head.term == term {
			return head.cb
		}
		// Because of the lack of original RaftCmdRequest, we skip calling
		// coprocessor here.
		notifyStaleCommand(regionID, peerID, term, *head)
	}
	return nil
}

func (d *applyDelegate) processRaftCmd(aCtx *applyContext, index, term uint64, cmd *raft_cmdpb.RaftCmdRequest) applyResult {
	if index == 0 {
		panic(fmt.Sprintf("%s process raft cmd need a none zero index", d.tag))
	}
	if cmd.AdminRequest != nil {
		aCtx.syncLogHint = true
	}
	isConfChange := GetChangePeerCmd(cmd) != nil
	aCtx.host.preApply(d.region, cmd)
	resp, result := d.applyRaftCmd(aCtx, index, term, cmd)
	if result.tp == applyResultTypeWaitMergeResource {
		return result
	}
	log.Debugf("applied command. region_id %d, peer_id %d, index %d", d.region.Id, d.id, index)

	// TODO: if we have exec_result, maybe we should return this callback too. Outer
	// store will call it after handing exec result.
	BindRespTerm(resp, term)
	cmdCB := d.findCallback(index, term, isConfChange)
	aCtx.cbs[len(aCtx.cbs)-1].push(cmdCB, resp)
	return result
}

/// Applies raft command.
///
/// An apply operation can fail in the following situations:
///   1. it encounters an error that will occur on all stores, it can continue
/// applying next entry safely, like epoch not match for example;
///   2. it encounters an error that may not occur on all stores, in this case
/// we should try to apply the entry again or panic. Considering that this
/// usually due to disk operation fail, which is rare, so just panic is ok.
func (d *applyDelegate) applyRaftCmd(aCtx *applyContext, index, term uint64,
	req *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.RaftCmdResponse, applyResult) {
	// if pending remove, apply should be aborted already.
	y.Assert(!d.pendingRemove)

	aCtx.execCtx = d.newCtx(index, term)
	aCtx.wb.SetSafePoint()
	resp, applyResult, err := d.execRaftCmd(aCtx, req)
	if err != nil {
		// clear dirty values.
		aCtx.wb.RollbackToSafePoint()
		if _, ok := err.(*ErrEpochNotMatch); ok {
			log.Debugf("epoch not match region_id %d, peer_id %d, err %v", d.region.Id, d.id, err)
		} else {
			log.Errorf("execute raft command region_id %d, peer_id %d, err %v", d.region.Id, d.id, err)
		}
		resp = ErrResp(err)
	}
	if applyResult.tp == applyResultTypeWaitMergeResource {
		return resp, applyResult
	}
	d.applyState = aCtx.execCtx.applyState
	aCtx.execCtx = nil
	d.applyState.appliedIndex = index
	d.appliedIndexTerm = term

	if applyResult.tp == applyResultTypeExecResult {
		switch x := applyResult.data.(type) {
		case *execResultChangePeer:
			d.region = x.cp.region
		case *execResultSplitRegion:
			d.region = x.derived
			d.metrics.sizeDiffHint = 0
			d.metrics.deleteKeysHint = 0
		case *execResultPrepareMerge:
			d.region = x.region
			d.isMerging = true
		case *execResultCommitMerge:
			d.region = x.region
			d.lastMergeVersion = x.region.RegionEpoch.Version
		case *execResultRollbackMerge:
			d.region = x.region
			d.isMerging = false
		default:
		}
	}
	return resp, applyResult
}

func (d *applyDelegate) destroy(aCtx *applyContext) {
	d.stopped = true
	aCtx.router.close(d.region.Id)
	for _, cmd := range d.pendingCmds.normals {
		notifyRegionRemoved(d.region.Id, d.id, cmd)
	}
	d.pendingCmds.normals = nil
	if cmd := d.pendingCmds.takeConfChange(); cmd != nil {
		notifyRegionRemoved(d.region.Id, d.id, *cmd)
	}
}

func (d *applyDelegate) clearAllCommandsAsStale() {
	for _, cmd := range d.pendingCmds.normals {
		notifyStaleCommand(d.region.Id, d.id, d.term, cmd)
	}
	d.pendingCmds.normals = d.pendingCmds.normals[:0]
	if cmd := d.pendingCmds.takeConfChange(); cmd != nil {
		notifyStaleCommand(d.region.Id, d.id, d.term, *cmd)
	}
}

func (d *applyDelegate) newCtx(index, term uint64) *applyExecContext {
	return &applyExecContext{
		index:      index,
		term:       term,
		applyState: d.applyState,
	}
}

// Only errors that will also occur on all other stores should be returned.
func (d *applyDelegate) execRaftCmd(aCtx *applyContext, req *raft_cmdpb.RaftCmdRequest) (
	resp *raft_cmdpb.RaftCmdResponse, result applyResult, err error) {
	// Include region for epoch not match after merge may cause key not in range.
	includeRegion := req.Header.GetRegionEpoch().GetVersion() >= d.lastMergeVersion
	err = checkRegionEpoch(req, d.region, includeRegion)
	if err != nil {
		return
	}
	if req.AdminRequest != nil {
		return d.execAdminCmd(aCtx, req)
	}
	return d.execWriteCmd(aCtx, req)
}

func (d *applyDelegate) execAdminCmd(aCtx *applyContext, req *raft_cmdpb.RaftCmdRequest) (
	resp *raft_cmdpb.RaftCmdResponse, result applyResult, err error) {
	adminReq := req.AdminRequest
	cmdType := adminReq.CmdType
	if cmdType != raft_cmdpb.AdminCmdType_CompactLog && cmdType != raft_cmdpb.AdminCmdType_CommitMerge {
		log.Infof("%s execute admin command. term %d, index %d, command %s",
			d.tag, aCtx.execCtx.term, aCtx.execCtx.index, adminReq)
	}
	var adminResp *raft_cmdpb.AdminResponse
	switch cmdType {
	case raft_cmdpb.AdminCmdType_ChangePeer:
		adminResp, result, err = d.execChangePeer(aCtx, adminReq)
	case raft_cmdpb.AdminCmdType_Split:
		adminResp, result, err = d.execSplit(aCtx, adminReq)
	case raft_cmdpb.AdminCmdType_BatchSplit:
		adminResp, result, err = d.execBatchSplit(aCtx, adminReq)
	case raft_cmdpb.AdminCmdType_CompactLog:
		adminResp, result, err = d.execCompactLog(aCtx, adminReq)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		err = errors.New("transfer leader won't execute")
	case raft_cmdpb.AdminCmdType_ComputeHash:
		adminResp, result, err = d.execComputeHash(aCtx, adminReq)
	case raft_cmdpb.AdminCmdType_VerifyHash:
		adminResp, result, err = d.execVerifyHash(aCtx, adminReq)
	case raft_cmdpb.AdminCmdType_PrepareMerge:
		adminResp, result, err = d.execPrepareMerge(aCtx, adminReq)
	case raft_cmdpb.AdminCmdType_CommitMerge:
		adminResp, result, err = d.execCommitMerge(aCtx, adminReq)
	case raft_cmdpb.AdminCmdType_RollbackMerge:
		adminResp, result, err = d.execRollbackMerge(aCtx, adminReq)
	case raft_cmdpb.AdminCmdType_InvalidAdmin:
		err = errors.New("unsupported command type")
	}
	if err != nil {
		return
	}
	adminResp.CmdType = cmdType
	resp = newCmdRespForReq(req)
	resp.AdminResponse = adminResp
	return
}

func (d *applyDelegate) execWriteCmd(aCtx *applyContext, req *raft_cmdpb.RaftCmdRequest) (
	resp *raft_cmdpb.RaftCmdResponse, result applyResult, err error) {
	requests := req.GetRequests()
	writeCmdOps := d.createWriteCmdOps(requests)
	for _, op := range writeCmdOps.prewrites {
		d.execPrewrite(aCtx, op)
	}
	for _, op := range writeCmdOps.commits {
		d.execCommit(aCtx, op)
	}
	for _, op := range writeCmdOps.rollbacks {
		d.execRollback(aCtx, op)
	}
	for _, op := range writeCmdOps.delRanges {
		d.execDeleteRange(aCtx, op)
	}
	resps := make([]raft_cmdpb.Response, len(requests))
	respPtrs := make([]*raft_cmdpb.Response, len(requests))
	for i := 0; i < len(resps); i++ {
		resp := &resps[i]
		resp.CmdType = requests[i].CmdType
		respPtrs[i] = resp
	}
	resp = newCmdRespForReq(req)
	resp.Responses = respPtrs
	if len(writeCmdOps.delRanges) > 0 {
		result = applyResult{
			tp:   applyResultTypeExecResult,
			data: &execResultDeleteRange{},
		}
	}
	return
}

// every commit must followed with a delete lock.
type commitOp struct {
	putWrite *raft_cmdpb.PutRequest
	delLock  *raft_cmdpb.DeleteRequest
}

// a prewrite may optionally has a put Default.
// a put default must follows a put lock.
type prewriteOp struct {
	putDefault *raft_cmdpb.PutRequest // optional
	putLock    *raft_cmdpb.PutRequest
}

// a Rollback optionally has a delete Default.
type rollbackOp struct {
	delDefault *raft_cmdpb.DeleteRequest
	putWrite   *raft_cmdpb.PutRequest
	delLock    *raft_cmdpb.DeleteRequest
}

type writeCmdOps struct {
	prewrites []prewriteOp
	commits   []commitOp
	rollbacks []rollbackOp
	delRanges []*raft_cmdpb.DeleteRangeRequest
}

// createWriteCmdOps regroups requests into operations.
func (d *applyDelegate) createWriteCmdOps(requests []*raft_cmdpb.Request) (ops writeCmdOps) {
	// If first request is delete write, then this is a GC command, we can ignore it.
	if del := requests[0].Delete; del != nil {
		if del.Cf == CFWrite {
			return
		}
	}
	for i := 0; i < len(requests); i++ {
		req := requests[i]
		switch req.CmdType {
		case raft_cmdpb.CmdType_Delete:
			del := req.Delete
			switch del.Cf {
			case "":
				// Rollback large value, must followed by put write and delete lock
				putWriteReq := requests[i+1]
				y.Assert(putWriteReq.Put != nil && putWriteReq.Put.Cf == CFWrite)
				delLockReq := requests[i+2]
				y.Assert(delLockReq.Delete != nil && delLockReq.Delete.Cf == CFLock)
				ops.rollbacks = append(ops.rollbacks, rollbackOp{
					delDefault: req.Delete,
					putWrite:   putWriteReq.Put,
					delLock:    delLockReq.Delete,
				})
				i += 2
			case CFWrite:
				// This is collapse rollback, since we do local rollback GC, we can ignore it.
			default:
				panic("unreachable")
			}
		case raft_cmdpb.CmdType_Put:
			put := req.Put
			switch put.Cf {
			case "":
				// Prewrite with large value, the next req must be put lock.
				nextPut := requests[i+1].Put
				y.Assert(nextPut != nil && nextPut.Cf == CFLock)
				ops.prewrites = append(ops.prewrites, prewriteOp{
					putDefault: put,
					putLock:    nextPut,
				})
				i++
			case CFLock:
				// Prewrite with short value.
				ops.prewrites = append(ops.prewrites, prewriteOp{putLock: put})
			case CFWrite:
				writeType := put.Value[0]
				if writeType == mvcc.WriteTypeRollback {
					// Rollback optionally followed by a delete lock.
					var nextDel *raft_cmdpb.DeleteRequest
					if i+1 < len(requests) {
						if tmpDel := requests[i+1].Delete; tmpDel != nil && tmpDel.Cf == CFLock {
							nextDel = tmpDel
						}
					}
					if nextDel != nil {
						ops.rollbacks = append(ops.rollbacks, rollbackOp{
							putWrite: put,
							delLock:  nextDel,
						})
						i++
					} else {
						ops.rollbacks = append(ops.rollbacks, rollbackOp{
							putWrite: put,
						})
					}
				} else {
					// Commit must followed by del lock
					nextDel := requests[i+1].Delete
					y.Assert(nextDel != nil && nextDel.Cf == CFLock)
					ops.commits = append(ops.commits, commitOp{
						putWrite: put,
						delLock:  nextDel,
					})
					i++
				}
			}
		case raft_cmdpb.CmdType_DeleteRange:
			ops.delRanges = append(ops.delRanges, req.DeleteRange)
		case raft_cmdpb.CmdType_IngestSST:
			panic("ingestSST not unsupported")
		case raft_cmdpb.CmdType_Snap, raft_cmdpb.CmdType_Get:
			// Readonly commands are handled in raftstore directly.
			// Don't panic here in case there are old entries need to be applied.
			// It's also safe to skip them here, because a restart must have happened,
			// hence there is no callback to be called.
			log.Warnf("%s skip read-only command %s", d.tag, req)
		default:
			panic("unreachable")
		}
	}
	return
}

func (d *applyDelegate) execPrewrite(aCtx *applyContext, op prewriteOp) {
	_, rawKey, err := codec.DecodeBytes(op.putLock.Key, nil)
	if err != nil {
		panic(op.putLock.Key)
	}
	lock, err := mvcc.ParseLockCFValue(op.putLock.Value)
	if err != nil {
		panic(op.putLock.Value)
	}
	txn := aCtx.getTxn()
	if item, err := txn.Get(rawKey); err == nil {
		val, err1 := item.Value()
		if err1 != nil {
			panic(err1)
		}
		lock.HasOldVer = true
		lock.OldMeta = item.UserMeta()
		lock.OldVal = val
	}
	if op.putDefault != nil {
		lock.Value = op.putDefault.Value
	}
	aCtx.wb.SetLock(rawKey, lock.MarshalBinary())
	return
}

func (d *applyDelegate) execCommit(aCtx *applyContext, op commitOp) {
	remain, rawKey, err := codec.DecodeBytes(op.putWrite.Key, nil)
	if err != nil {
		panic(op.putWrite.Key)
	}
	_, commitTS, err := codec.DecodeUintDesc(remain)
	if err != nil {
		panic(remain)
	}
	val := d.getLock(aCtx, rawKey)
	y.Assert(len(val) > 0)
	lock := mvcc.DecodeLock(val)
	userMeta := mvcc.NewDBUserMeta(lock.StartTS, commitTS)
	aCtx.wb.SetWithUserMeta(rawKey, lock.Value, userMeta)
	if lock.HasOldVer {
		oldKey := mvcc.EncodeOldKey(rawKey, lock.OldMeta.CommitTS())
		aCtx.wb.SetWithUserMeta(oldKey, lock.OldVal, lock.OldMeta.ToOldUserMeta(commitTS))
	}
	if op.delLock != nil {
		aCtx.wb.DeleteLock(rawKey)
	}
	return
}

func (d *applyDelegate) getLock(aCtx *applyContext, rawKey []byte) []byte {
	if val := aCtx.engines.kv.lockStore.Get(rawKey, nil); len(val) > 0 {
		return val
	}
	lockEntries := aCtx.wb.lockEntries
	for i := len(lockEntries) - 1; i >= 0; i-- {
		if bytes.Equal(lockEntries[i].Key, rawKey) {
			return lockEntries[i].Value
		}
	}
	return nil
}

func (d *applyDelegate) execRollback(aCtx *applyContext, op rollbackOp) {
	remain, rawKey, err := codec.DecodeBytes(op.putWrite.Key, nil)
	if err != nil {
		panic(op.putWrite.Key)
	}
	aCtx.wb.Rollback(append(rawKey, remain...))
	if op.delLock != nil {
		aCtx.wb.DeleteLock(rawKey)
	}
	return
}

func (d *applyDelegate) execDeleteRange(aCtx *applyContext, req *raft_cmdpb.DeleteRangeRequest) {
	_, startKey, err := codec.DecodeBytes(req.StartKey, nil)
	if err != nil {
		panic(req.StartKey)
	}
	_, endKey, err := codec.DecodeBytes(req.EndKey, nil)
	if err != nil {
		panic(req.EndKey)
	}
	txn := aCtx.getTxn()
	it := dbreader.NewIterator(txn, false, startKey, endKey)
	for it.Seek(startKey); it.Valid(); it.Next() {
		item := it.Item()
		if bytes.Compare(item.Key(), endKey) >= 0 {
			break
		}
		aCtx.wb.Delete(item.KeyCopy(nil))
	}
	it.Close()
	oldStartKey := mvcc.EncodeOldKey(startKey, 0)
	oldEndKey := mvcc.EncodeOldKey(endKey, 0)
	it = dbreader.NewIterator(txn, false, oldStartKey, oldEndKey)
	for it.Seek(oldStartKey); it.Valid(); it.Next() {
		item := it.Item()
		if bytes.Compare(item.Key(), oldEndKey) >= 0 {
			break
		}
		aCtx.wb.Delete(item.KeyCopy(nil))
	}
	it.Close()
	lockIt := aCtx.engines.kv.lockStore.NewIterator()
	for lockIt.Seek(startKey); lockIt.Valid(); lockIt.Next() {
		if bytes.Compare(lockIt.Key(), endKey) >= 0 {
			break
		}
		aCtx.wb.DeleteLock(safeCopy(lockIt.Key()))
	}
	return
}

func (d *applyDelegate) execChangePeer(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	request := req.ChangePeer
	peer := request.Peer
	storeID := peer.StoreId
	changeType := request.ChangeType
	region := new(metapb.Region)
	err = CloneMsg(d.region, region)
	if err != nil {
		return
	}
	log.Infof("%s exec ConfChange, peer_id %d, type %s, epoch %s",
		d.tag, peer.Id, changeType, region.RegionEpoch)

	// TODO: we should need more check, like peer validation, duplicated id, etc.
	region.RegionEpoch.ConfVer++

	switch changeType {
	case eraftpb.ConfChangeType_AddNode:
		var exist bool
		if p := findPeer(region, storeID); p != nil {
			exist = true
			if !p.IsLearner || p.Id != peer.Id {
				errMsg := fmt.Sprintf("%s can't add duplicated peer, peer %s, region %s",
					d.tag, p, d.region)
				log.Error(errMsg)
				err = errors.New(errMsg)
				return
			}
			p.IsLearner = false
		}
		if !exist {
			// TODO: Do we allow adding peer in same node?
			region.Peers = append(region.Peers, peer)
		}
		log.Infof("%s add peer successfully, peer %s, region %s", d.tag, peer, d.region)
	case eraftpb.ConfChangeType_RemoveNode:
		if p := removePeer(region, storeID); p != nil {
			if !PeerEqual(p, peer) {
				errMsg := fmt.Sprintf("%s ignore remove unmatched peer, expected_peer %s, got_peer %s",
					d.tag, peer, p)
				log.Error(errMsg)
				err = errors.New(errMsg)
				return
			}
			if d.id == peer.Id {
				// Remove ourself, we will destroy all region data later.
				// So we need not to apply following logs.
				d.stopped = true
				d.pendingRemove = true
			}
		} else {
			errMsg := fmt.Sprintf("%s removing missing peers, peer %s, region %s",
				d.tag, peer, d.region)
			log.Error(errMsg)
			err = errors.New(errMsg)
			return
		}
		log.Infof("%s remove peer successfully, peer %s, region %s", d.tag, peer, d.region)
	case eraftpb.ConfChangeType_AddLearnerNode:
		if findPeer(region, storeID) != nil {
			errMsg := fmt.Sprintf("%s can't add duplicated learner, peer %s, region %s",
				d.tag, peer, d.region)
			log.Error(errMsg)
			err = errors.New(errMsg)
			return
		}
		region.Peers = append(region.Peers, peer)
		log.Infof("%s add learner successfully, peer %s, region %s", d.tag, peer, d.region)
	}
	state := rspb.PeerState_Normal
	if d.pendingRemove {
		state = rspb.PeerState_Tombstone
	}
	WritePeerState(aCtx.wb, region, state, nil)
	resp = &raft_cmdpb.AdminResponse{
		ChangePeer: &raft_cmdpb.ChangePeerResponse{
			Region: region,
		},
	}
	result = applyResult{
		tp: applyResultTypeExecResult,
		data: &execResultChangePeer{
			cp: changePeer{
				confChange: new(eraftpb.ConfChange),
				region:     region,
				peer:       peer,
			},
		},
	}
	return
}

func (d *applyDelegate) execSplit(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	split := req.Split
	adminReq := &raft_cmdpb.AdminRequest{
		Splits: &raft_cmdpb.BatchSplitRequest{
			Requests:    []*raft_cmdpb.SplitRequest{split},
			RightDerive: split.RightDerive,
		},
	}
	// This method is executed only when there are unapplied entries after being restarted.
	// So there will be no callback, it's OK to return a response that does not matched
	// with its request.
	return d.execBatchSplit(aCtx, adminReq)
}

func (d *applyDelegate) execBatchSplit(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	splitReqs := req.Splits
	rightDerive := splitReqs.RightDerive
	if len(splitReqs.Requests) == 0 {
		err = errors.New("missing split key")
		return
	}
	derived := new(metapb.Region)
	if err := CloneMsg(d.region, derived); err != nil {
		panic(err)
	}
	newRegionCnt := len(splitReqs.Requests)
	regions := make([]*metapb.Region, 0, newRegionCnt+1)
	keys := make([][]byte, 0, newRegionCnt+1)
	keys = append(keys, derived.StartKey)
	for _, request := range splitReqs.Requests {
		splitKey := request.SplitKey
		if len(splitKey) == 0 {
			err = errors.New("missing split key")
			return
		}
		if bytes.Compare(splitKey, keys[len(keys)-1]) <= 0 {
			err = errors.Errorf("invalid split request:%s", splitReqs)
			return
		}
		if len(request.NewPeerIds) != len(derived.Peers) {
			err = errors.Errorf("invalid new peer id count, need %d but got %d",
				len(derived.Peers), len(request.NewPeerIds))
			return
		}
		keys = append(keys, splitKey)
	}
	keys = append(keys, derived.EndKey)
	err = CheckKeyInRegion(keys[len(keys)-2], d.region)
	if err != nil {
		return
	}
	log.Infof("%s split region %s, keys %v", d.tag, d.region, keys)
	derived.RegionEpoch.Version += uint64(newRegionCnt)
	// Note that the split requests only contain ids for new regions, so we need
	// to handle new regions and old region separately.
	if !rightDerive {
		derived.EndKey = keys[1]
		keys = keys[1:]
		regions = append(regions, derived)
	}
	for i, request := range splitReqs.Requests {
		newRegion := &metapb.Region{
			Id:          request.NewRegionId,
			RegionEpoch: derived.RegionEpoch,
			StartKey:    keys[i],
			EndKey:      keys[i+1],
		}
		newRegion.Peers = make([]*metapb.Peer, len(derived.Peers))
		for j := range newRegion.Peers {
			newRegion.Peers[j] = &metapb.Peer{
				Id:        request.NewPeerIds[j],
				StoreId:   derived.Peers[j].StoreId,
				IsLearner: derived.Peers[j].IsLearner,
			}
		}
		WritePeerState(aCtx.wb, newRegion, rspb.PeerState_Normal, nil)
		writeInitialApplyState(aCtx.wb, newRegion.Id)
		regions = append(regions, newRegion)
	}
	if rightDerive {
		derived.StartKey = keys[len(keys)-2]
		regions = append(regions, derived)
	}
	WritePeerState(aCtx.wb, derived, rspb.PeerState_Normal, nil)

	resp = &raft_cmdpb.AdminResponse{
		Splits: &raft_cmdpb.BatchSplitResponse{
			Regions: regions,
		},
	}
	result = applyResult{tp: applyResultTypeExecResult, data: &execResultSplitRegion{
		regions: regions,
		derived: derived,
	}}
	return
}

func (d *applyDelegate) execPrepareMerge(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	return // TODO: merge
}

func (d *applyDelegate) execCommitMerge(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	return // TODO: merge
}

func (d *applyDelegate) execRollbackMerge(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	return // TODO: merge
}

func (d *applyDelegate) execCompactLog(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	compactIndex := req.CompactLog.CompactIndex
	resp = new(raft_cmdpb.AdminResponse)
	applyState := aCtx.execCtx.applyState
	firstIndex := firstIndex(applyState)
	if compactIndex <= firstIndex {
		log.Debugf("%s compact index <= first index, no need to compact", d.tag)
		return
	}
	if d.isMerging {
		log.Debugf("%s in merging mode, skip compact", d.tag)
		return
	}
	compactTerm := req.CompactLog.CompactTerm
	if compactTerm == 0 {
		log.Infof("%s compact term missing, skip", d.tag)
		// old format compact log command, safe to ignore.
		err = errors.New("command format is outdated, please upgrade leader")
		return
	}

	// compact failure is safe to be omitted, no need to assert.
	err = CompactRaftLog(d.tag, &applyState, compactIndex, compactTerm)
	if err != nil {
		return
	}
	result = applyResult{tp: applyResultTypeExecResult, data: &execResultCompactLog{
		truncatedIndex: applyState.truncatedIndex,
		firstIndex:     firstIndex,
	}}
	return
}

func (d *applyDelegate) execComputeHash(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	resp = new(raft_cmdpb.AdminResponse)
	result = applyResult{tp: applyResultTypeExecResult, data: &execResultComputeHash{
		region: d.region,
		index:  aCtx.execCtx.index,
		// This snapshot may be held for a long time, which may cause too many
		// open files in rocksdb.
		// TODO: figure out another way to do consistency check without snapshot
		// or short life snapshot.
		snap: NewDBSnapshot(aCtx.engines.kv),
	}}
	return
}

func (d *applyDelegate) execVerifyHash(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	verifyReq := req.VerifyHash
	resp = new(raft_cmdpb.AdminResponse)
	result = applyResult{tp: applyResultTypeExecResult, data: &execResultVerifyHash{
		index: verifyReq.Index,
		hash:  verifyReq.Hash,
	}}
	return
}

type catchUpLogs struct {
	targetMailBox *mailbox
	merge         *raft_cmdpb.CommitMergeRequest
	readyToMerge  *atomic.Uint64
}

type applyPollerBuilder struct {
	tag             string
	cfg             *Config
	coprocessorHost *CoprocessorHost
	engines         *Engines
	sender          notifier
	router          *applyRouter
	regionScheduler chan<- task
}

func newApplyPollerBuilder(raftPollerBuilder *raftPollerBuilder, sender notifier, router *applyRouter, regionScheduler chan<- task) *applyPollerBuilder {
	return &applyPollerBuilder{
		tag:             fmt.Sprintf("[store %d]", raftPollerBuilder.store.Id),
		cfg:             raftPollerBuilder.cfg,
		coprocessorHost: raftPollerBuilder.coprocessorHost,
		engines:         raftPollerBuilder.engines,
		sender:          sender,
		router:          router,
		regionScheduler: regionScheduler,
	}
}

func (b *applyPollerBuilder) build() pollHandler {
	return &applyPoller{
		msgBuf:          make([]Msg, 0, b.cfg.MessagesPerTick),
		aCtx:            newApplyContext(b.tag, b.coprocessorHost, b.regionScheduler, b.engines, b.router, b.sender, b.cfg),
		messagesPerTick: int(b.cfg.MessagesPerTick),
	}
}

type applyFsm struct {
	applyDelegate
	receiver <-chan Msg
	mailbox  *mailbox
}

func newApplyFsmFromPeer(peer *peerFsm) (chan<- Msg, fsm) {
	reg := newRegistration(peer.peer)
	return newApplyFsmFromRegistration(reg)
}

func newApplyFsmFromRegistration(reg *registration) (chan<- Msg, fsm) {
	ch := make(chan Msg, msgDefaultChanSize)
	delegate := newApplyDelegate(reg)
	aFsm := &applyFsm{
		applyDelegate: *delegate,
		receiver:      ch,
	}
	return ch, aFsm
}

/// Handles peer registration. When a peer is created, it will register an apply delegate.
func (a *applyFsm) handleRegistration(reg *registration) {
	log.Infof("%s re-register to apply delegate, term %d", a.tag, reg.term)
	y.Assert(a.id == reg.id)
	a.term = reg.term
	a.clearAllCommandsAsStale()
	a.applyDelegate = *newApplyDelegate(reg)
}

/// Handles apply tasks, and uses the apply delegate to handle the committed entries.
func (a *applyFsm) handleApply(aCtx *applyContext, apply *apply) {
	if aCtx.timer == nil {
		now := time.Now()
		aCtx.timer = &now
	}
	if len(apply.entries) == 0 || a.pendingRemove || a.stopped {
		return
	}
	a.metrics = applyMetrics{}
	a.term = apply.term
	a.handleRaftCommittedEntries(aCtx, apply.entries)
	apply.entries = apply.entries[:0]
	if a.waitMergeState != nil {
		return
	}
	if a.pendingRemove {
		a.destroy(aCtx)
	}
}

/// Handles proposals, and appends the commands to the apply delegate.
func (a *applyFsm) handleProposal(regionProposal *regionProposal) {
	regionID, peerID := a.region.Id, a.id
	y.Assert(a.id == regionProposal.Id)
	if a.stopped {
		for _, p := range regionProposal.Props {
			cmd := pendingCmd{index: p.index, term: p.term, cb: p.cb}
			notifyStaleCommand(regionID, peerID, a.term, cmd)
		}
		return
	}
	for _, p := range regionProposal.Props {
		cmd := pendingCmd{index: p.index, term: p.term, cb: p.cb}
		if p.isConfChange {
			if confCmd := a.pendingCmds.takeConfChange(); confCmd != nil {
				// if it loses leadership before conf change is replicated, there may be
				// a stale pending conf change before next conf change is applied. If it
				// becomes leader again with the stale pending conf change, will enter
				// this block, so we notify leadership may have been changed.
				notifyStaleCommand(regionID, peerID, a.term, *confCmd)
			}
			a.pendingCmds.setConfChange(&cmd)
		} else {
			a.pendingCmds.appendNormal(cmd)
		}
	}
}

func (a *applyFsm) destroy(aCtx *applyContext) {
	regionID := a.region.Id
	for _, res := range aCtx.applyTaskResList {
		if res.regionID == regionID {
			// Flush before destroying to avoid reordering messages.
			aCtx.flush()
		}
	}
	log.Infof("%s remove delegate from apply delegate", a.tag)
	a.applyDelegate.destroy(aCtx)
}

/// Handles peer destroy. When a peer is destroyed, the corresponding apply delegate should be removed too.
func (a *applyFsm) handleDestroy(aCtx *applyContext, regionID uint64) {
	if !a.stopped {
		a.destroy(aCtx)
		aCtx.notifier.notify(regionID, NewPeerMsg(MsgTypeApplyRes, a.region.Id, &applyTaskRes{
			regionID:      a.region.Id,
			destroyPeerID: a.id,
		}))
	}
}

func (a *applyFsm) resumePendingMerge(aCtx *applyContext) bool {
	return false // TODO: merge
}

func (a *applyFsm) catchUpLogsForMerge(aCtx *applyContext, logs *catchUpLogs) {
	// TODO: merge
}

func (a *applyFsm) handleSnapshot(aCtx *applyContext, snapTask *GenSnapTask) {
	if a.applyDelegate.pendingRemove || a.applyDelegate.stopped {
		return
	}
	regionID := a.applyDelegate.region.GetId()
	for _, res := range aCtx.applyTaskResList {
		if res.regionID == regionID {
			aCtx.flush()
			break
		}
	}
	snapTask.generateAndScheduleSnapshot(aCtx.regionScheduler)
}

func (a *applyFsm) handleTasks(aCtx *applyContext, msgs []Msg) {
	for i, msg := range msgs {
		switch msg.Type {
		case MsgTypeApply:
			a.handleApply(aCtx, msg.Data.(*apply))
			if a.waitMergeState != nil {
				a.waitMergeState.pendingMsgs = msgs[i+1:]
				break
			}
		case MsgTypeApplyProposal:
			a.handleProposal(msg.Data.(*regionProposal))
		case MsgTypeApplyRegistration:
			a.handleRegistration(msg.Data.(*registration))
		case MsgTypeApplyDestroy:
			a.handleDestroy(aCtx, msg.Data.(uint64))
		case MsgTypeApplyCatchUpLogs:
			a.catchUpLogsForMerge(aCtx, msg.Data.(*catchUpLogs))
		case MsgTypeApplyLogsUpToDate:
		case MsgTypeApplySnapshot:
			a.handleSnapshot(aCtx, msg.Data.(*GenSnapTask))
		}
	}
}

func (a *applyFsm) isStopped() bool {
	return a.stopped
}

func (a *applyFsm) setMailbox(mb *mailbox) {
	a.mailbox = mb
}

func (a *applyFsm) takeMailbox() *mailbox {
	mb := a.mailbox
	a.mailbox = nil
	return mb
}

type applyPoller struct {
	msgBuf          []Msg
	aCtx            *applyContext
	messagesPerTick int
}

func (p *applyPoller) begin(batchSize int) {}

/// There is no control fsm in apply poller.
func (p *applyPoller) handleControl(control fsm) (pause bool, chLen int) {
	panic("unimplemented")
}

func (p *applyPoller) handleNormal(normal fsm) (pause bool, chLen int) {
	applyFsm := normal.(*applyFsm)
	receiverLen := len(applyFsm.receiver)
	if applyFsm.waitMergeState != nil {
		// We need to query the length first, otherwise there is a race
		// condition that new messages are queued after resuming and before
		// query the length.
		if !applyFsm.resumePendingMerge(p.aCtx) {
			return true, receiverLen
		}
	}
	numToRecv := p.messagesPerTick - len(p.msgBuf)
	if numToRecv >= receiverLen {
		numToRecv = receiverLen
	}
	for i := 0; i < numToRecv; i++ {
		p.msgBuf = append(p.msgBuf, <-applyFsm.receiver)
	}
	applyFsm.handleTasks(p.aCtx, p.msgBuf)
	p.msgBuf = nil
	if applyFsm.merged {
		applyFsm.destroy(p.aCtx)
	}
	return false, 0
}

func (p *applyPoller) end(fsms []fsm) {
	p.aCtx.flush()
}

func createApplyBatchSystem(cfg *Config) (*applyRouter, *batchSystem) {
	router, system := createBatchSystem(int(cfg.ApplyPoolSize), int(cfg.ApplyMaxBatchSize), nil, nil)
	applyRouter := &applyRouter{router: *router}
	return applyRouter, system
}

package raftstore

import (
	"fmt"
	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/uber-go/atomic"
	"time"
)

const (
	WriteBatchMaxKeys  = 128
	DefaultApplyWBSize = 4 * 1024
)

type pendingCmd struct {
	index uint64
	term  uint64
	cb    Callback
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
	cbs    []callBackResponsePair
}

func (c *applyCallback) invokeAll(host *CoprocessorHost) {
	for _, cb := range c.cbs {
		host.postApply(c.region, cb.response)
		if cb.callBack != nil {
			cb.callBack(cb.response, nil)
		}
	}
}

func (c *applyCallback) push(cb Callback, resp *raft_cmdpb.RaftCmdResponse) {
	c.cbs = append(c.cbs, callBackResponsePair{callBack: cb, response: resp})
}

type callBackResponsePair struct {
	callBack Callback
	response *raft_cmdpb.RaftCmdResponse
}

type proposal struct {
	isConfChange bool
	index        uint64
	term         uint64
	cb           Callback
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
	router           *applyRouter
	notifier         *notifier
	engines          *Engines
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

func newApplyContext(tag string, host *CoprocessorHost, engines *Engines, router *applyRouter,
	notifier *notifier, cfg *Config) *applyContext {
	return &applyContext{
		tag:            tag,
		host:           host,
		engines:        engines,
		router:         router,
		notifier:       notifier,
		enableSyncLog:  cfg.SyncLog,
		useDeleteRange: cfg.UseDeleteRange,
		wb:             new(WriteBatch),
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
		if err := ac.wb.WriteToDB(ac.engines.kv); err != nil {
			panic(err)
		}
		ac.wb.Reset()
	}
	for _, cb := range ac.cbs {
		cb.invokeAll(ac.host)
	}
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

func (ac *applyContext) flush() {
	// TODO: this check is too hacky, need to be more verbose and less buggy.
	t := ac.timer
	ac.timer = nil
	if t == nil {
		return
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

func notifyReqRegionRemoved(regionID uint64, cb Callback) {
	resp := ErrRespRegionNotFound(regionID)
	cb(resp, nil)
}

/// Calls the callback of `cmd` when it can not be processed further.
func notifyStaleCommand(regionID, peerID, term uint64, cmd pendingCmd) {
	log.Infof("command is stale, skip. regionID %d, peerID %d, index %d, term %d",
		regionID, peerID, cmd.index, cmd.term)
	notifyStaleReq(term, cmd.cb)
}

func notifyStaleReq(term uint64, cb Callback) {
	resp := ErrRespStaleCommand(term)
	cb(resp, nil)
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
	pendingEntries []*eraftpb.Entry
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
func (d *applyDelegate) handleRaftCommittedEntries(aCtx *applyContext, committedEntries []*eraftpb.Entry) {
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
	for i, entry := range committedEntries {
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
			pendingEntries := make([]*eraftpb.Entry, 0, len(committedEntries)-i)
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
		cb.cbs = append(cb.cbs, callBackResponsePair{
			callBack: cmd.cb,
			response: ErrRespStaleCommand(term),
		})
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

func (d *applyDelegate) findCallback(index, term uint64, isConfChange bool) Callback {
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
	isConfChange := getChangePeerCmd(cmd) != nil
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
	includeRegion := req.Header.RegionEpoch.Version >= d.lastMergeVersion
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
	return // TODO: stub
}

func (d *applyDelegate) execWriteCmd(aCtx *applyContext, req *raft_cmdpb.RaftCmdRequest) (
	resp *raft_cmdpb.RaftCmdResponse, result applyResult, err error) {
	return // TODO: stub
}

func (d *applyDelegate) execPut(aCtx *applyContext, req *raft_cmdpb.Request) (
	resp *raft_cmdpb.Response, err error) {
	return // TODO: stub
}

func (d *applyDelegate) execDelete(aCtx *applyContext, req *raft_cmdpb.Request) (
	resp *raft_cmdpb.Response, err error) {
	return // TODO: stub
}

func (d *applyDelegate) execDeleteRange(aCtx *applyContext, req *raft_cmdpb.Request,
	ranges []keyRange, useDeleteRange bool) (resp *raft_cmdpb.Response, err error) {
	return // TODO: stub
}

func (d *applyDelegate) execChangePeer(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	return // TODO: stub
}

func (d *applyDelegate) execSplit(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	return // TODO: stub
}

func (d *applyDelegate) execBatchSplit(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	return // TODO: stub
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
	return // TODO: stub
}

func (d *applyDelegate) execComputeHash(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	return // TODO: stub
}

func (d *applyDelegate) execVerifyHash(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	return // TODO: stub
}

func getChangePeerCmd(msg *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.ChangePeerRequest {
	return nil // TODO: stub
}

/// Updates the `state` with given `compact_index` and `compact_term`.
///
/// Remember the Raft log is not deleted here.
func compactRaftLog(tag string, state *applyState, compactIndex, compactTerm uint64) error {
	return nil // TODO: stub
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
}

func newApplyPollerBuilder(raftPollerBuilder *raftPollerBuilder, sender notifier, router *applyRouter) *applyPollerBuilder {
	return nil // TODO: stub
}

func (b *applyPollerBuilder) build() pollHandler {
	return nil // TODO: stub
}

type applyFsm struct {
	applyDelegate
	receiver <-chan Msg
	mailbox  *mailbox
}

func newApplyFsmFromPeer(peer *peerFsm) (chan<- Msg, fsm) {
	return nil, nil // TODO: stub
}

func newApplyFsmFromRegistration(reg *registration) (chan<- Msg, fsm) {
	return nil, nil // TODO: stub
}

/// Handles peer registration. When a peer is created, it will register an apply delegate.
func (a *applyFsm) handleRegistration(reg *registration) {
	// TODO: stub
}

/// Handles apply tasks, and uses the apply delegate to handle the committed entries.
func (a *applyFsm) handleApply(aCtx *applyContext, apply *apply) {
	// TODO: stub
}

/// Handles proposals, and appends the commands to the apply delegate.
func (a *applyFsm) handleProposal(regionProposal *regionProposal) {
	// TODO: stub
}

func (a *applyFsm) destroy(aCtx *applyContext) {
	// TODO: stub
}

/// Handles peer destroy. When a peer is destroyed, the corresponding apply delegate should be removed too.
func (a *applyFsm) handleDestroy(aCtx *applyContext, regionID uint64) {
	// TODO: stub
}

func (a *applyFsm) resumePendingMerge(aCtx *applyContext) bool {
	return false // TODO: merge
}

func (a *applyFsm) catchUpLogsForMerge(aCtx *applyContext, logs *catchUpLogs) {
	// TODO: merge
}

func (a *applyFsm) handleTasks(aCtx *applyContext, msgs []Msg) {
	// TODO: stub
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
	messagesPerTick uint64
}

func (p *applyPoller) begin(batchSize int) {}

/// There is no control fsm in apply poller.
func (p *applyPoller) handleControl(control fsm) (pause bool, chLen int) {
	panic("unimplemented")
}

func (p *applyPoller) handleNormal(normal fsm) (pause bool, chLen int) {
	return // TODO: stub
}

func (p *applyPoller) end(fsms []fsm) {
	// TODO: stub
}

func createApplyBatchSystem(cfg *Config) (*applyRouter, *batchSystem) {
	return nil, nil // TODO: stub
}

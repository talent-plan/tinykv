package raftstore

import (
	"bytes"
	"fmt"
	"time"

	"github.com/coocood/badger"
	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
	"github.com/pingcap-incubator/tinykv/kv/tikv/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/codec"
)

const (
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

type applyTaskRes struct {
	regionID         uint64
	applyState       applyState
	appliedIndexTerm uint64
	execResults      []execResult
	sizeDiffHint     uint64

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

type execResult = interface{}

type applyResultType int

const (
	applyResultTypeNone       applyResultType = 0
	applyResultTypeExecResult applyResultType = 1
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

func (c *applyCallback) invokeAll(doneApplyTime time.Time) {
	for _, cb := range c.cbs {
		if cb != nil {
			cb.wg.Done()
		}
	}
}

func (c *applyCallback) push(cb *Callback, resp *raft_cmdpb.RaftCmdResponse) {
	if cb != nil {
		cb.resp = resp
	}
	c.cbs = append(c.cbs, cb)
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

func (t *GenSnapTask) generateAndScheduleSnapshot(regionSched chan<- task, redoIdx uint64) {
	regionSched <- task{
		tp: taskTypeRegionGen,
		data: &regionTask{
			regionId: t.regionID,
			notifier: t.snapNotifier,
			redoIdx:  redoIdx,
		},
	}
}

type applyMsgs struct {
	msgs []Msg
}

func (r *applyMsgs) appendMsg(regionID uint64, msg Msg) {
	msg.RegionID = regionID
	r.msgs = append(r.msgs, msg)
	return
}

type applyContext struct {
	tag              string
	timer            *time.Time
	regionScheduler  chan<- task
	notifier         chan<- Msg
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

func newApplyContext(tag string, regionScheduler chan<- task, engines *Engines,
	notifier chan<- Msg, cfg *Config) *applyContext {
	return &applyContext{
		tag:             tag,
		regionScheduler: regionScheduler,
		engines:         engines,
		notifier:        notifier,
		enableSyncLog:   cfg.SyncLog,
		useDeleteRange:  cfg.UseDeleteRange,
		wb:              new(WriteBatch),
	}
}

/// Prepares for applying entries for `applier`.
///
/// A general apply progress for an applier is:
/// `prepare_for` -> `commit` [-> `commit` ...] -> `finish_for`.
/// After all appliers are handled, `write_to_db` method should be called.
func (ac *applyContext) prepareFor(d *applier) {
	if ac.wb == nil {
		ac.wb = new(WriteBatch)
		ac.wbLastBytes = 0
		ac.wbLastKeys = 0
	}
	ac.cbs = append(ac.cbs, applyCallback{region: d.region})
	ac.lastAppliedIndex = d.applyState.appliedIndex
}

/// Commits all changes have done for applier. `persistent` indicates whether
/// write the changes into rocksdb.
///
/// This call is valid only when it's between a `prepare_for` and `finish_for`.
func (ac *applyContext) commit(d *applier) {
	if ac.lastAppliedIndex < d.applyState.appliedIndex {
		d.writeApplyState(ac.wb)
	}
	// last_applied_index doesn't need to be updated, set persistent to true will
	// force it call `prepare_for` automatically.
	ac.commitOpt(d, true)
}

func (ac *applyContext) commitOpt(d *applier, persistent bool) {
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
		ac.wbLastBytes = 0
		ac.wbLastKeys = 0
	}
	doneApply := time.Now()
	for _, cb := range ac.cbs {
		cb.invokeAll(doneApply)
	}
	ac.cbs = ac.cbs[:0]
}

/// Finishes `Apply`s for the applier.
func (ac *applyContext) finishFor(d *applier, results []execResult) {
	if !d.pendingRemove {
		d.writeApplyState(ac.wb)
	}
	ac.commitOpt(d, false)
	res := &applyTaskRes{
		regionID:         d.region.Id,
		applyState:       d.applyState,
		execResults:      results,
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
		ac.txn = ac.engines.kv.DB.NewTransaction(false)
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
			ac.notifier <- NewPeerMsg(MsgTypeApplyRes, res.regionID, res)
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

/// The applier of a Region which is responsible for handling committed
/// raft log entries of a Region.
///
/// `Apply` is a term of Raft, which means executing the actual commands.
/// In Raft, once some log entries are committed, for every peer of the Raft
/// group will apply the logs one by one. For write commands, it does write or
/// delete to local engine; for admin commands, it does some meta change of the
/// Raft group.
///
/// The raft worker receives all the apply tasks of different Regions
/// located at this store, and it will get the corresponding applier to
/// handle the apply task to make the code logic more clear.
type applier struct {
	id     uint64
	term   uint64
	region *metapb.Region
	tag    string

	/// If the applier should be stopped from polling.
	/// A applier can be stopped in conf change, merge or requested by destroy message.
	stopped bool
	/// Set to true when removing itself because of `ConfChangeType::RemoveNode`, and then
	/// any following committed logs in same Ready should be applied failed.
	pendingRemove bool

	/// The commands waiting to be committed and applied
	pendingCmds pendingCmdQueue

	/// We writes apply_state to KV DB, in one write batch together with kv data.
	///
	/// If we write it to Raft DB, apply_state and kv data (Put, Delete) are in
	/// separate WAL file. When power failure, for current raft log, apply_index may synced
	/// to file, but KV data may not synced to file, so we will lose data.
	applyState applyState
	/// The term of the raft log at applied index.
	appliedIndexTerm uint64

	// redoIdx is the raft log index starts redo for lockStore.
	redoIndex uint64

	sizeDiffHint uint64
}

func newApplier(reg *registration) *applier {
	return &applier{
		id:               reg.id,
		tag:              fmt.Sprintf("[region %d] %d", reg.region.Id, reg.id),
		region:           reg.region,
		applyState:       reg.applyState,
		appliedIndexTerm: reg.appliedIndexTerm,
		term:             reg.term,
	}
}

/// Handles all the committed_entries, namely, applies the committed entries.
func (a *applier) handleRaftCommittedEntries(aCtx *applyContext, committedEntries []eraftpb.Entry) {
	if len(committedEntries) == 0 {
		return
	}
	aCtx.prepareFor(a)
	aCtx.committedCount += len(committedEntries)
	// If we send multiple ConfChange commands, only first one will be proposed correctly,
	// others will be saved as a normal entry with no data, so we must re-propose these
	// commands again.
	aCtx.committedCount += len(committedEntries)
	var results []execResult
	for i := range committedEntries {
		entry := &committedEntries[i]
		if a.pendingRemove {
			// This peer is about to be destroyed, skip everything.
			break
		}
		expectedIndex := a.applyState.appliedIndex + 1
		if expectedIndex != entry.Index {
			panic(fmt.Sprintf("%s expect index %d, but got %d", a.tag, expectedIndex, entry.Index))
		}
		var res applyResult
		switch entry.EntryType {
		case eraftpb.EntryType_EntryNormal:
			res = a.handleRaftEntryNormal(aCtx, entry)
		case eraftpb.EntryType_EntryConfChange:
			res = a.handleRaftEntryConfChange(aCtx, entry)
		}
		switch res.tp {
		case applyResultTypeNone:
		case applyResultTypeExecResult:
			results = append(results, res.data)
		}
	}
	aCtx.finishFor(a, results)
}

func (a *applier) writeApplyState(wb *WriteBatch) {
	wb.Set(ApplyStateKey(a.region.Id), a.applyState.Marshal())
}

func (a *applier) handleRaftEntryNormal(aCtx *applyContext, entry *eraftpb.Entry) applyResult {
	index := entry.Index
	term := entry.Term
	if len(entry.Data) > 0 {
		cmd := new(raft_cmdpb.RaftCmdRequest)
		err := cmd.Unmarshal(entry.Data)
		if err != nil {
			panic(err)
		}
		return a.processRaftCmd(aCtx, index, term, cmd)
	}

	// when a peer become leader, it will send an empty entry.
	a.applyState.appliedIndex = index
	a.appliedIndexTerm = term
	y.Assert(term > 0)
	for {
		cmd := a.pendingCmds.popNormal(term - 1)
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

func (a *applier) handleRaftEntryConfChange(aCtx *applyContext, entry *eraftpb.Entry) applyResult {
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
	result := a.processRaftCmd(aCtx, index, term, cmd)
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

func (a *applier) findCallback(index, term uint64, isConfChange bool) *Callback {
	regionID := a.region.Id
	peerID := a.id
	if isConfChange {
		cmd := a.pendingCmds.takeConfChange()
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
		head := a.pendingCmds.popNormal(term)
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

func (a *applier) processRaftCmd(aCtx *applyContext, index, term uint64, cmd *raft_cmdpb.RaftCmdRequest) applyResult {
	if index == 0 {
		panic(fmt.Sprintf("%s process raft cmd need a none zero index", a.tag))
	}
	if cmd.AdminRequest != nil {
		aCtx.syncLogHint = true
	}
	isConfChange := GetChangePeerCmd(cmd) != nil
	resp, result := a.applyRaftCmd(aCtx, index, term, cmd)
	log.Debugf("applied command. region_id %d, peer_id %d, index %d", a.region.Id, a.id, index)

	// TODO: if we have exec_result, maybe we should return this callback too. Outer
	// store will call it after handing exec result.
	BindRespTerm(resp, term)
	cmdCB := a.findCallback(index, term, isConfChange)
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
func (a *applier) applyRaftCmd(aCtx *applyContext, index, term uint64,
	req *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.RaftCmdResponse, applyResult) {
	// if pending remove, apply should be aborted already.
	y.Assert(!a.pendingRemove)

	aCtx.execCtx = a.newCtx(index, term)
	aCtx.wb.SetSafePoint()
	resp, applyResult, err := a.execRaftCmd(aCtx, req)
	if err != nil {
		// clear dirty values.
		aCtx.wb.RollbackToSafePoint()
		if _, ok := err.(*ErrEpochNotMatch); ok {
			log.Debugf("epoch not match region_id %d, peer_id %d, err %v", a.region.Id, a.id, err)
		} else {
			log.Errorf("execute raft command region_id %d, peer_id %d, err %v", a.region.Id, a.id, err)
		}
		resp = ErrResp(err)
	}
	a.applyState = aCtx.execCtx.applyState
	aCtx.execCtx = nil
	a.applyState.appliedIndex = index
	a.appliedIndexTerm = term

	if applyResult.tp == applyResultTypeExecResult {
		switch x := applyResult.data.(type) {
		case *execResultChangePeer:
			a.region = x.cp.region
		case *execResultSplitRegion:
			a.region = x.derived
		default:
		}
	}
	return resp, applyResult
}

func (a *applier) clearAllCommandsAsStale() {
	for _, cmd := range a.pendingCmds.normals {
		notifyStaleCommand(a.region.Id, a.id, a.term, cmd)
	}
	a.pendingCmds.normals = a.pendingCmds.normals[:0]
	if cmd := a.pendingCmds.takeConfChange(); cmd != nil {
		notifyStaleCommand(a.region.Id, a.id, a.term, *cmd)
	}
}

func (a *applier) newCtx(index, term uint64) *applyExecContext {
	return &applyExecContext{
		index:      index,
		term:       term,
		applyState: a.applyState,
	}
}

// Only errors that will also occur on all other stores should be returned.
func (a *applier) execRaftCmd(aCtx *applyContext, req *raft_cmdpb.RaftCmdRequest) (
	resp *raft_cmdpb.RaftCmdResponse, result applyResult, err error) {
	// Include region for epoch not match after merge may cause key not in range.
	err = checkRegionEpoch(req, a.region, false)
	if err != nil {
		return
	}
	if req.AdminRequest != nil {
		return a.execAdminCmd(aCtx, req)
	}
	return a.execWriteCmd(aCtx, req)
}

func (a *applier) execAdminCmd(aCtx *applyContext, req *raft_cmdpb.RaftCmdRequest) (
	resp *raft_cmdpb.RaftCmdResponse, result applyResult, err error) {
	adminReq := req.AdminRequest
	cmdType := adminReq.CmdType
	if cmdType != raft_cmdpb.AdminCmdType_CompactLog {
		log.Infof("%s execute admin command. term %d, index %d, command %s",
			a.tag, aCtx.execCtx.term, aCtx.execCtx.index, adminReq)
	}
	var adminResp *raft_cmdpb.AdminResponse
	switch cmdType {
	case raft_cmdpb.AdminCmdType_ChangePeer:
		adminResp, result, err = a.execChangePeer(aCtx, adminReq)
	case raft_cmdpb.AdminCmdType_BatchSplit:
		adminResp, result, err = a.execBatchSplit(aCtx, adminReq)
	case raft_cmdpb.AdminCmdType_CompactLog:
		adminResp, result, err = a.execCompactLog(aCtx, adminReq)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		err = errors.New("transfer leader won't execute")
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

func (a *applier) execWriteCmd(aCtx *applyContext, req *raft_cmdpb.RaftCmdRequest) (
	resp *raft_cmdpb.RaftCmdResponse, result applyResult, err error) {
	requests := req.GetRequests()
	writeCmdOps := createWriteCmdOps(requests)
	for _, op := range writeCmdOps {
		switch x := op.(type) {
		case *prewriteOp:
			a.execPrewrite(aCtx, *x)
		case *commitOp:
			a.execCommit(aCtx, *x)
		case *rollbackOp:
			a.execRollback(aCtx, *x)
		default:
			log.Fatalf("invalid input op=%v", x)
		}
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

// createWriteCmdOps regroups requests into operations.
func createWriteCmdOps(requests []*raft_cmdpb.Request) (ops []interface{}) {
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
				ops = append(ops, &rollbackOp{
					delDefault: req.Delete,
					putWrite:   putWriteReq.Put,
					delLock:    delLockReq.Delete,
				})
				i += 2
			case CFWrite:
				// This is collapse rollback, since we do local rollback GC, we can ignore it.
			case CFLock:
				// This is pessimistic rollback.
				ops = append(ops, &rollbackOp{
					delLock: req.Delete,
				})
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
				ops = append(ops, &prewriteOp{
					putDefault: put,
					putLock:    nextPut,
				})
				i++
			case CFLock:
				// Prewrite with short value.
				ops = append(ops, &prewriteOp{putLock: put})
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
						ops = append(ops, &rollbackOp{
							putWrite: put,
							delLock:  nextDel,
						})
						i++
					} else {
						ops = append(ops, &rollbackOp{
							putWrite: put,
						})
					}
				} else {
					// Commit must followed by del lock
					nextDel := requests[i+1].Delete
					y.Assert(nextDel != nil && nextDel.Cf == CFLock)
					ops = append(ops, &commitOp{
						putWrite: put,
						delLock:  nextDel,
					})
					i++
				}
			}
		case raft_cmdpb.CmdType_Snap, raft_cmdpb.CmdType_Get:
			// Readonly commands are handled in raftstore directly.
			// Don't panic here in case there are old entries need to be applied.
			// It's also safe to skip them here, because a restart must have happened,
			// hence there is no callback to be called.
			log.Warnf("skip read-only command %s", req)
		default:
			panic("unreachable")
		}
	}
	return
}

func (a *applier) execPrewrite(aCtx *applyContext, op prewriteOp) {
	key, value := convertPrewriteToLock(op, aCtx.getTxn())
	aCtx.wb.SetLock(key, value)
	return
}

func convertPrewriteToLock(op prewriteOp, txn *badger.Txn) (key, value []byte) {
	_, rawKey, err := codec.DecodeBytes(op.putLock.Key, nil)
	if err != nil {
		panic(op.putLock.Key)
	}
	lock, err := mvcc.ParseLockCFValue(op.putLock.Value)
	if err != nil {
		panic(op.putLock.Value)
	}
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
	return rawKey, lock.MarshalBinary()
}

func (a *applier) execCommit(aCtx *applyContext, op commitOp) {
	remain, rawKey, err := codec.DecodeBytes(op.putWrite.Key, nil)
	if err != nil {
		panic(op.putWrite.Key)
	}
	_, commitTS, err := codec.DecodeUintDesc(remain)
	if err != nil {
		panic(remain)
	}
	val := a.getLock(aCtx, rawKey)
	y.Assert(len(val) > 0)
	lock := mvcc.DecodeLock(val)
	var sizeDiff int64
	userMeta := mvcc.NewDBUserMeta(lock.StartTS, commitTS)
	if lock.Op != uint8(kvrpcpb.Op_Lock) {
		aCtx.wb.SetWithUserMeta(rawKey, lock.Value, userMeta)
		sizeDiff = int64(len(rawKey) + len(lock.Value))
		if lock.HasOldVer {
			oldKey := mvcc.EncodeOldKey(rawKey, lock.OldMeta.CommitTS())
			aCtx.wb.SetWithUserMeta(oldKey, lock.OldVal, lock.OldMeta.ToOldUserMeta(commitTS))
			sizeDiff -= int64(len(rawKey) + len(lock.OldVal))
		}
	} else if bytes.Equal(lock.Primary, rawKey) {
		// For primary key with Op_Lock type, the value need to be skipped, but we need to keep the transaction status.
		// So we put it as old key directly.
		if lock.HasOldVer {
			// There is a latest value, we don't want to move the value to the old key space for performance and simplicity.
			// Write the lock as old key directly to store the transaction status.
			// The old entry doesn't have value as Delete entry, but we can compare the commitTS in key and NextCommitTS
			// in the user meta to determine if the entry is Delete or Op_Lock.
			// If NextCommitTS equals CommitTS, it is Op_Lock, otherwise, it is Delete.
			oldKey := mvcc.EncodeOldKey(rawKey, commitTS)
			aCtx.wb.SetWithUserMeta(oldKey, nil, userMeta)
		} else {
			// Convert the lock to a delete to store the transaction status.
			aCtx.wb.SetWithUserMeta(rawKey, nil, userMeta)
		}
	}
	if sizeDiff > 0 {
		a.sizeDiffHint += uint64(sizeDiff)
	}
	if op.delLock != nil {
		aCtx.wb.DeleteLock(rawKey, val)
	}
	return
}

func (a *applier) getLock(aCtx *applyContext, rawKey []byte) []byte {
	if val := aCtx.engines.kv.LockStore.Get(rawKey, nil); len(val) > 0 {
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

func (a *applier) execRollback(aCtx *applyContext, op rollbackOp) {
	if op.putWrite != nil {
		remain, rawKey, err := codec.DecodeBytes(op.putWrite.Key, nil)
		if err != nil {
			panic(op.putWrite.Key)
		}
		aCtx.wb.Rollback(append(rawKey, remain...))
		if op.delLock != nil {
			aCtx.wb.DeleteLock(rawKey, a.getLock(aCtx, rawKey))
		}
		return
	}
	if op.delLock != nil {
		_, rawKey, err := codec.DecodeBytes(op.delLock.Key, nil)
		if err != nil {
			panic(op.delLock.Key)
		}
		aCtx.wb.DeleteLock(rawKey, a.getLock(aCtx, rawKey))
	}
}

func (a *applier) execChangePeer(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	request := req.ChangePeer
	peer := request.Peer
	storeID := peer.StoreId
	changeType := request.ChangeType
	region := new(metapb.Region)
	err = CloneMsg(a.region, region)
	if err != nil {
		return
	}
	log.Infof("%s exec ConfChange, peer_id %d, type %s, epoch %s",
		a.tag, peer.Id, changeType, region.RegionEpoch)

	// TODO: we should need more check, like peer validation, duplicated id, etc.
	region.RegionEpoch.ConfVer++

	switch changeType {
	case eraftpb.ConfChangeType_AddNode:
		var exist bool
		if p := findPeer(region, storeID); p != nil {
			exist = true
			if !p.IsLearner || p.Id != peer.Id {
				errMsg := fmt.Sprintf("%s can't add duplicated peer, peer %s, region %s",
					a.tag, p, a.region)
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
		log.Infof("%s add peer successfully, peer %s, region %s", a.tag, peer, a.region)
	case eraftpb.ConfChangeType_RemoveNode:
		if p := removePeer(region, storeID); p != nil {
			if !PeerEqual(p, peer) {
				errMsg := fmt.Sprintf("%s ignore remove unmatched peer, expected_peer %s, got_peer %s",
					a.tag, peer, p)
				log.Error(errMsg)
				err = errors.New(errMsg)
				return
			}
			if a.id == peer.Id {
				// Remove ourself, we will destroy all region data later.
				// So we need not to apply following logs.
				a.stopped = true
				a.pendingRemove = true
			}
		} else {
			errMsg := fmt.Sprintf("%s removing missing peers, peer %s, region %s",
				a.tag, peer, a.region)
			log.Error(errMsg)
			err = errors.New(errMsg)
			return
		}
		log.Infof("%s remove peer successfully, peer %s, region %s", a.tag, peer, a.region)
	case eraftpb.ConfChangeType_AddLearnerNode:
		if findPeer(region, storeID) != nil {
			errMsg := fmt.Sprintf("%s can't add duplicated learner, peer %s, region %s",
				a.tag, peer, a.region)
			log.Error(errMsg)
			err = errors.New(errMsg)
			return
		}
		region.Peers = append(region.Peers, peer)
		log.Infof("%s add learner successfully, peer %s, region %s", a.tag, peer, a.region)
	}
	state := rspb.PeerState_Normal
	if a.pendingRemove {
		state = rspb.PeerState_Tombstone
	}
	WritePeerState(aCtx.wb, region, state)
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

func (a *applier) execBatchSplit(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	splitReqs := req.Splits
	if len(splitReqs.Requests) == 0 {
		err = errors.New("missing split key")
		return
	}
	derived := new(metapb.Region)
	if err := CloneMsg(a.region, derived); err != nil {
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
	err = CheckKeyInRegion(keys[len(keys)-2], a.region)
	if err != nil {
		return
	}
	log.Infof("%s split region %s, keys %v", a.tag, a.region, keys)
	derived.RegionEpoch.Version += uint64(newRegionCnt)
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
		WritePeerState(aCtx.wb, newRegion, rspb.PeerState_Normal)
		writeInitialApplyState(aCtx.wb, newRegion.Id)
		regions = append(regions, newRegion)
	}
	derived.StartKey = keys[len(keys)-2]
	regions = append(regions, derived)
	WritePeerState(aCtx.wb, derived, rspb.PeerState_Normal)

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

func (a *applier) execCompactLog(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	compactIndex := req.CompactLog.CompactIndex
	resp = new(raft_cmdpb.AdminResponse)
	applyState := &aCtx.execCtx.applyState
	firstIndex := firstIndex(*applyState)
	if compactIndex <= firstIndex {
		log.Debugf("%s compact index <= first index, no need to compact", a.tag)
		return
	}
	compactTerm := req.CompactLog.CompactTerm
	if compactTerm == 0 {
		log.Infof("%s compact term missing, skip", a.tag)
		// old format compact log command, safe to ignore.
		err = errors.New("command format is outdated, please upgrade leader")
		return
	}

	// compact failure is safe to be omitted, no need to assert.
	err = CompactRaftLog(a.tag, applyState, compactIndex, compactTerm)
	if err != nil {
		return
	}
	result = applyResult{tp: applyResultTypeExecResult, data: &execResultCompactLog{
		truncatedIndex: applyState.truncatedIndex,
		firstIndex:     firstIndex,
	}}
	return
}

func newApplierFromPeer(peer *peerFsm) *applier {
	reg := newRegistration(peer.peer)
	return newApplier(reg)
}

/// Handles peer registration. When a peer is created, it will register an applier.
func (a *applier) handleRegistration(reg *registration) {
	log.Infof("%s re-register to applier, term %d", a.tag, reg.term)
	y.Assert(a.id == reg.id)
	a.term = reg.term
	a.clearAllCommandsAsStale()
	*a = *newApplier(reg)
}

/// Handles apply tasks, and uses the applier to handle the committed entries.
func (a *applier) handleApply(aCtx *applyContext, apply *apply) {
	if aCtx.timer == nil {
		now := time.Now()
		aCtx.timer = &now
	}
	if len(apply.entries) == 0 || a.pendingRemove || a.stopped {
		return
	}
	a.term = apply.term
	a.handleRaftCommittedEntries(aCtx, apply.entries)
	apply.entries = apply.entries[:0]
	if a.pendingRemove {
		a.destroy(aCtx)
	}
}

/// Handles proposals, and appends the commands to the applier.
func (a *applier) handleProposal(regionProposal *regionProposal) {
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

func (a *applier) destroy(aCtx *applyContext) {
	regionID := a.region.Id
	for _, res := range aCtx.applyTaskResList {
		if res.regionID == regionID {
			// Flush before destroying to avoid reordering messages.
			aCtx.flush()
		}
	}
	log.Infof("%s remove applier", a.tag)
	a.stopped = true
	for _, cmd := range a.pendingCmds.normals {
		notifyRegionRemoved(a.region.Id, a.id, cmd)
	}
	a.pendingCmds.normals = nil
	if cmd := a.pendingCmds.takeConfChange(); cmd != nil {
		notifyRegionRemoved(a.region.Id, a.id, *cmd)
	}
}

/// Handles peer destroy. When a peer is destroyed, the corresponding applier should be removed too.
func (a *applier) handleDestroy(aCtx *applyContext, regionID uint64) {
	if !a.stopped {
		a.destroy(aCtx)
		aCtx.notifier <- NewPeerMsg(MsgTypeApplyRes, a.region.Id, &applyTaskRes{
			regionID:      a.region.Id,
			destroyPeerID: a.id,
		})
	}
}

func (a *applier) handleGenSnapshot(aCtx *applyContext, snapTask *GenSnapTask) {
	if a.pendingRemove || a.stopped {
		return
	}
	regionID := a.region.GetId()
	for _, res := range aCtx.applyTaskResList {
		if res.regionID == regionID {
			aCtx.flush()
			break
		}
	}
	snapTask.generateAndScheduleSnapshot(aCtx.regionScheduler, a.redoIndex)
}

func (a *applier) handleTask(aCtx *applyContext, msg Msg) {
	switch msg.Type {
	case MsgTypeApply:
		a.handleApply(aCtx, msg.Data.(*apply))
	case MsgTypeApplyProposal:
		a.handleProposal(msg.Data.(*regionProposal))
	case MsgTypeApplyRegistration:
		a.handleRegistration(msg.Data.(*registration))
	case MsgTypeApplyDestroy:
		a.handleDestroy(aCtx, msg.RegionID)
	case MsgTypeApplySnapshot:
		a.handleGenSnapshot(aCtx, msg.Data.(*GenSnapTask))
	}
}

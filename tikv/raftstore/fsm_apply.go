package raftstore

import (
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/uber-go/atomic"
	"time"
)

type pendingCmd struct {
	index uint64
	term  uint64
	cb    Callback
}

type pendingCmdQueue struct {
	normals    []*pendingCmd
	confChange *pendingCmd
}

func (q *pendingCmdQueue) popNormal(term uint64) *pendingCmd {
	if len(q.normals) == 0 {
		return nil
	}
	cmd := q.normals[0]
	if cmd.term > term {
		return nil
	}
	q.normals = q.normals[1:]
	return cmd
}

func (q *pendingCmdQueue) appendNormal(cmd *pendingCmd) {
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
	applyState       rspb.RaftApplyState
	appliedIndexTerm uint64
	execResults      []execResult
	metrics          *applyMetrics
	merged           bool

	destroyPeerID uint64
}

type execResultChangePeer struct {
	cp changePeer
}

type execResultCompactLog struct {
	state      *rspb.RaftTruncatedState
	firstIndex uint64
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

type execResult struct {
	data interface{}
}

type applyResultType int

const (
	applyResultTypeNone applyResultType = iota
	applyResultTypeExecResult
	applyResultTypeWaitMergeResource
)

type applyResult struct {
	tp   applyResultType
	data interface{}
}

type applyExecContext struct {
	index      uint64
	term       uint64
	applyState *rspb.RaftApplyState
}

type applyCallback struct {
	region *metapb.Region
	cbs    []callBackResponsePair
}

func (c *applyCallback) invokeAll(host *CoprocessorHost) {
	for _, cb := range c.cbs {
		host.poseApply(c.region, cb.response)
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
	applyState       *rspb.RaftApplyState
	appliedIndexTerm uint64
	region           *metapb.Region
}

func newRegistration(peer *Peer) *registration {
	return &registration{
		id:               peer.PeerId(),
		term:             peer.Term(),
		applyState:       CloneRaftApplyState(peer.Store().applyState),
		appliedIndexTerm: peer.Store().appliedIndexTerm,
		region:           peer.Region(),
	}
}

type applyRouter struct {
	router
}

func (r *applyRouter) scheduleTask(regionId uint64, msg Msg) {
	// TODO: stub
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
	timer            time.Time
	host             *CoprocessorHost
	router           *applyRouter
	notifier         *notifier
	engines          *Engines
	cbs              []applyCallback
	applyTaskResList []applyTaskRes
	execCtx          *applyExecContext
	wb               *WriteBatch
	wbLastBytes      uint64
	wbLastKeys       uint64
	lastAppliedIndex uint64
	committedCount   uint64

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
	ac.lastAppliedIndex = d.applyState.AppliedIndex
}

/// Commits all changes have done for delegate. `persistent` indicates whether
/// write the changes into rocksdb.
///
/// This call is valid only when it's between a `prepare_for` and `finish_for`.
func (ac *applyContext) commit(d *applyDelegate) {
	// TODO: stub
}

func (ac *applyContext) commitOpt(d *applyDelegate, persistent bool) {
	// TODO: stub
}

/// Writes all the changes into badger.
func (ac *applyContext) writeToDB() {
	// TODO: stub
}

func (ac *applyContext) finishFor(d *applyDelegate, results []execResult) {
	// TODO: stub
}

func (ac *applyContext) deltaBytes() uint64 {
	return uint64(ac.wb.size) - ac.wbLastBytes
}

func (ac *applyContext) deltaKeys() uint64 {
	return uint64(len(ac.wb.entries)) - ac.wbLastKeys
}

func (ac *applyContext) flush() {
	// TODO: stub
}

/// Calls the callback of `cmd` when the Region is removed.
func notifyRegionRemoved(regionID, peerID uint64, cmd pendingCmd) {
	// TODO: stub
}

func notifyReqRegionRemoved(regionID uint64, cb Callback) {
	// TODO: stub
}

/// Calls the callback of `cmd` when it can not be processed further.
func notifyStaleCommand(regionID, peerID, term uint64, cmd pendingCmd) {
	// TODO: stub
}

func notifyStaleReq(term uint64, cb Callback) {
	// TODO: stub
}

/// Checks if a write is needed to be issued before handling the command.
func shouldWriteToEngine(cmd *raft_cmdpb.RaftCmdRequest, wbKeys uint64) bool {
	return false // TODO: stub
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
	return "" // TODO: stub
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
	applyState *rspb.RaftApplyState
	/// The term of the raft log at applied index.
	appliedIndexTerm uint64

	/// The local metrics, and it will be flushed periodically.
	metrics *applyMetrics
}

func newApplyDelegate(reg *registration) *applyDelegate {
	return nil // TODO: stub
}

/// Handles all the committed_entries, namely, applies the committed entries.
func (d *applyDelegate) handleRaftCommittedEntries(aCtx *applyContext, committedEntries []*eraftpb.Entry) {
	// TODO: stub
}

func (d *applyDelegate) updateMetrics(aCtx *applyContext) {
	d.metrics.writtenBytes += aCtx.deltaBytes()
	d.metrics.writtenKeys += aCtx.deltaKeys()
}

func (d *applyDelegate) writeApplyState(wb *WriteBatch) {
	// TODO: stub
}

func (d *applyDelegate) handleRaftEntryNormal(aCtx *applyContext, entry *eraftpb.Entry) applyResult {
	return applyResult{} // TODO: stub
}

func (d *applyDelegate) handleRaftEntryConfChange(aCtx *applyContext, entry *eraftpb.Entry) applyResult {
	return applyResult{} // TODO: stub
}

func (d *applyDelegate) findCallback(index, term uint64, isConfChange bool) Callback {
	return nil // TODO: stub
}

func (d *applyDelegate) processRaftCmd(aCtx *applyContext, index, term uint64, req *raft_cmdpb.RaftCmdRequest) applyResult {
	return applyResult{} // TODO: stub
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
	return nil, applyResult{} // TODO: stub
}

func (d *applyDelegate) destroy(aCtx *applyContext) {
	// TODO: stub
}

func (d *applyDelegate) clearAllCommandsAsStale() {
	// TODO: stub
}

func (d *applyDelegate) newCtx(index, term uint64) *applyExecContext {
	return nil // TODO: stub
}

// Only errors that will also occur on all other stores should be returned.
func (d *applyDelegate) execRaftCmd(aCtx *applyContext, req *raft_cmdpb.RaftCmdRequest) (
	resp *raft_cmdpb.RaftCmdResponse, result applyResult, err error) {
	return // TODO: stub
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
func compactRaftLog(tag string, state *rspb.RaftApplyState, compactIndex, compactTerm uint64) error {
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

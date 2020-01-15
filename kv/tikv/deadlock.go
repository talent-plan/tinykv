package tikv

import (
	"context"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/ngaut/log"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/util/lockwaiter"
	deadlockPb "github.com/pingcap-incubator/tinykv/proto/pkg/deadlock"
)

// Follower will send detection rpc to Leader
const (
	Follower = iota
	Leader
)

type DetectorServer struct {
	Detector *Detector
	role     int32
}

// DetectorClient is a util used for distributed deadlock detection
type DetectorClient struct {
	pdClient     pd.Client
	sendCh       chan *deadlockPb.DeadlockRequest
	waitMgr      *lockwaiter.Manager
	streamCli    deadlockPb.Deadlock_DetectClient
	streamCancel context.CancelFunc
	streamConn   *grpc.ClientConn
}

// getLeaderAddr will send request to pd to find out the
// current leader node for the first region
func (dt *DetectorClient) getLeaderAddr() (string, error) {
	// find first region from pd, get the first region leader
	ctx := context.Background()
	_, leaderPeer, err := dt.pdClient.GetRegion(ctx, []byte{})
	if err != nil {
		log.Errorf("get first region failed, err: %v", err)
		return "", err
	}
	leaderStoreMeta, err := dt.pdClient.GetStore(ctx, leaderPeer.GetStoreId())
	if err != nil {
		log.Errorf("get store=%d failed, err=%v", leaderPeer.GetStoreId(), err)
		return "", err
	}
	log.Warnf("getLeaderAddr leader_peer=%v addr=%s", leaderPeer, leaderStoreMeta.GetAddress())
	return leaderStoreMeta.GetAddress(), nil
}

// rebuildStreamClient builds connection to the first region leader,
// it's not thread safe and should be called only by `DetectorClient.Start` or `DetectorClient.SendReqLoop`
func (dt *DetectorClient) rebuildStreamClient() error {
	leaderArr, err := dt.getLeaderAddr()
	if err != nil {
		return err
	}
	cc, err := grpc.Dial(leaderArr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	if dt.streamConn != nil {
		dt.streamConn.Close()
	}
	dt.streamConn = cc
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := deadlockPb.NewDeadlockClient(cc).Detect(ctx)
	if err != nil {
		cancel()
		return err
	}
	log.Infof("build stream client successfully, leaderAddr=%s", leaderArr)
	dt.streamCli = stream
	dt.streamCancel = cancel
	go dt.recvLoop(dt.streamCli)
	return nil
}

// NewDeadlockDetector will create a new detector util, entryTTL is used for
// recycling the lock wait edge in detector wait wap. chSize is the pending
// detection sending worker.Task size(used on non leader node)
func NewDetectorClient(waiterMgr *lockwaiter.Manager, pdClient pd.Client) *DetectorClient {
	chSize := 10000
	newDetector := &DetectorClient{
		sendCh:   make(chan *deadlockPb.DeadlockRequest, chSize),
		waitMgr:  waiterMgr,
		pdClient: pdClient,
	}
	return newDetector
}

// Start starts the detection `send`, `recv` and `entry recycle` loop
func (dt *DetectorClient) Start() {
	go dt.sendReqLoop()
}

// sendReqLoop will send detection request to leader, stream connection will be rebuilt and
// a new recv goroutine using the same stream client will be created
func (dt *DetectorClient) sendReqLoop() {
	var (
		err        error
		rebuildErr error
		req        *deadlockPb.DeadlockRequest
	)
	for {
		if dt.streamCli == nil {
			rebuildErr = dt.rebuildStreamClient()
			if rebuildErr != nil {
				log.Errorf("rebuild connection to first region failed, err=%v", rebuildErr)
				time.Sleep(3 * time.Second)
				continue
			}
		}
		req = <-dt.sendCh
		err = dt.streamCli.Send(req)
		if err != nil {
			log.Warnf("send err=%v, invalid current stream and try to rebuild connection", err)
			dt.streamCancel()
			dt.streamCli = nil
		}
	}
}

// recvLoop tries to recv response(current only deadlock error) from leader, break loop if errors happen
func (dt *DetectorClient) recvLoop(streamCli deadlockPb.Deadlock_DetectClient) {
	var (
		err  error
		resp *deadlockPb.DeadlockResponse
	)
	for {
		resp, err = streamCli.Recv()
		if err != nil {
			log.Warnf("recv from failed, err=%v, stop receive", err)
			break
		}
		// here only detection request will get response from leader
		dt.waitMgr.WakeUpForDeadlock(resp)
	}
}

func (dt *DetectorClient) handleRemoteTask(requestType deadlockPb.DeadlockRequestType,
	txnTs uint64, waitForTxnTs uint64, keyHash uint64) {
	detectReq := &deadlockPb.DeadlockRequest{}
	detectReq.Tp = requestType
	detectReq.Entry.Txn = txnTs
	detectReq.Entry.WaitForTxn = waitForTxnTs
	detectReq.Entry.KeyHash = keyHash
	dt.sendCh <- detectReq
}

// user interfaces
// Cleanup processes cleaup worker.Task on local detector
func (dt *DetectorClient) CleanUp(startTs uint64) {
	dt.handleRemoteTask(deadlockPb.DeadlockRequestType_CleanUp, startTs, 0, 0)
}

// CleanUpWaitFor cleans up the specific wait edge in detector's wait map
func (dt *DetectorClient) CleanUpWaitFor(txnTs, waitForTxn, keyHash uint64) {
	dt.handleRemoteTask(deadlockPb.DeadlockRequestType_CleanUpWaitFor, txnTs, waitForTxn, keyHash)
}

// DetectRemote post the detection request to local deadlock detector or remote first region leader,
// the caller should use `waiter.ch` to receive possible deadlock response
func (dt *DetectorClient) Detect(txnTs uint64, waitForTxnTs uint64, keyHash uint64) {
	dt.handleRemoteTask(deadlockPb.DeadlockRequestType_Detect, txnTs, waitForTxnTs, keyHash)
}

// convertErrToResp converts `ErrDeadlock` to `DeadlockResponse` proto type
func convertErrToResp(errDeadlock *ErrDeadlock, txnTs, waitForTxnTs, keyHash uint64) *deadlockPb.DeadlockResponse {
	entry := deadlockPb.WaitForEntry{}
	entry.Txn = txnTs
	entry.WaitForTxn = waitForTxnTs
	entry.KeyHash = keyHash
	resp := &deadlockPb.DeadlockResponse{}
	resp.Entry = entry
	resp.DeadlockKeyHash = errDeadlock.DeadlockKeyHash
	return resp
}

// NewDetectorServer creates local detector used by RPC detection handler
func NewDetectorServer() *DetectorServer {
	entryTTL := time.Duration(3 * time.Second)
	urgentSize := uint64(100000)
	exipreInterval := 3600 * time.Second
	svr := &DetectorServer{
		Detector: NewDetector(entryTTL, urgentSize, exipreInterval),
	}
	return svr
}

func (ds *DetectorServer) IsLeader() bool {
	return atomic.LoadInt32(&ds.role) == Leader
}

func (ds *DetectorServer) ChangeRole(newRole int32) {
	atomic.StoreInt32(&ds.role, newRole)
}

package exec

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/interfaces"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"sync"
)

// Sequential is a Scheduler which executes all commands sequentially on a single thread. Since there is no concurrency,
// no latching, etc. is required.
type Sequential struct {
	innerServer interfaces.InnerServer
	queue       chan task
	// Before modifying any property of a key, the thread must have the latch for that key. `latches` maps each latched
	// key to a WaitGroup. Threads who find a key locked should wait on that WaitGroup.
	latches map[string]*sync.WaitGroup
	// Mutex to guard latches. A thread must hold this mutex while it makes any change to latches.
	latchGuard sync.Mutex
}

// task is a task to be run by a scheduler.
type task struct {
	cmd           interfaces.Command
	resultChannel chan<- interfaces.SchedResult
}

// NewSeqScheduler creates a new sequential scheduler.
func NewSeqScheduler(innerServer interfaces.InnerServer) *Sequential {
	sched := &Sequential{innerServer: innerServer, queue: make(chan task), latches: make(map[string]*sync.WaitGroup)}
	go sched.handleTask()
	return sched
}

// handleTask takes a task from Sequential's queue and executes it.
func (seq *Sequential) handleTask() {
	for {
		task := <-seq.queue

		// A nil task is a command to stop and close down the scheduler.
		if task.cmd == nil && task.resultChannel == nil {
			close(seq.queue)
			return
		}

		// Get the data we need for execution.
		ctxt := task.cmd.Context()
		reader, err := seq.innerServer.Reader(ctxt)
		if handleError(err, task, nil) {
			continue
		}

		// Latch all keys required by the command. Make sure to call releaseLatches.
		latches, err := task.cmd.WillWrite(reader)
		if handleError(err, task, reader) {
			continue
		}
		for {
			wg := seq.acquireLatches(latches)
			if wg == nil {
				break
			}
			wg.Wait()
		}

		// Build an mvcc transaction.
		txn := kvstore.NewTxn(reader)
		err = task.cmd.BuildTxn(&txn)
		if handleError(err, task, reader) {
			seq.releaseLatches(latches)
			continue
		}

		// Building the transaction succeeded without conflict, write all writes to backing storage.
		err = seq.innerServer.Write(ctxt, txn.Writes)
		seq.releaseLatches(latches)
		if handleError(err, task, reader) {
			continue
		}

		// Send response back to the gRPC thread.
		task.resultChannel <- interfaces.RespOk(task.cmd.Response())

		reader.Close()
		close(task.resultChannel)
	}
}

// acquireLatches locks all latches needed to execute txn.
func (seq *Sequential) acquireLatches(keys [][]byte) *sync.WaitGroup {
	seq.latchGuard.Lock()
	defer seq.latchGuard.Unlock()

	// Check none of the keys we want to write are locked.
	for _, key := range keys {
		if latchWg, ok := seq.latches[string(key)]; ok {
			// Return a wait group to wait on.
			return latchWg
		}
	}

	// All latches are available, lock them all with a new wait group.
	wg := new(sync.WaitGroup)
	wg.Add(1)
	for _, key := range keys {
		seq.latches[string(key)] = wg
	}

	return nil
}

func (seq *Sequential) releaseLatches(keys [][]byte) {
	seq.latchGuard.Lock()
	defer seq.latchGuard.Unlock()

	first := true
	for _, key := range keys {
		if first {
			wg := seq.latches[string(key)]
			wg.Done()
			first = false
		}
		delete(seq.latches, string(key))
	}
}

// Give the command in task an opportunity to handle err. Returns true if there was an error so the caller is done with
// this command.
func handleError(err error, task task, reader dbreader.DBReader) bool {
	if err == nil {
		return false
	}

	if resp := task.cmd.HandleError(err); resp != nil {
		task.resultChannel <- interfaces.RespOk(resp)
	} else {
		task.resultChannel <- interfaces.RespErr(err)
	}

	if reader != nil {
		reader.Close()
	}
	close(task.resultChannel)
	return true
}

func (seq *Sequential) Stop() {
	seq.queue <- task{nil, nil}
}

func (seq *Sequential) Run(cmd interfaces.Command) <-chan interfaces.SchedResult {
	channel := make(chan interfaces.SchedResult, 1)
	tsk := task{cmd, channel}
	seq.queue <- tsk
	return channel
}

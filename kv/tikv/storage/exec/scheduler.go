package exec

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
)

// Sequential is a Scheduler which executes all commands sequentially on a single thread. Since there is no concurrency,
// no latching, etc. is required.
type Sequential struct {
	innerServer tikv.InnerServer
	queue       chan task
}

// task is a task to be run by a scheduler.
type task struct {
	cmd           tikv.Command
	resultChannel chan<- tikv.RespResult
}

// NewSeqScheduler creates a new sequential scheduler.
func NewSeqScheduler(innerServer tikv.InnerServer) *Sequential {
	sched := &Sequential{innerServer, make(chan task)}
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
		// TODO reader should be a snapshot of the underlying data at a given timestamp, not a generic reader.
		reader, err := seq.innerServer.Reader(ctxt)
		if handleError(err, task) {
			continue
		}

		// Build an mvcc transaction.
		txn := kvstore.NewTxn(reader)
		err = task.cmd.BuildTxn(&txn)
		if handleError(err, task) {
			continue
		}

		// Building the transaction succeeded without conflict, write all its writes to backing storage (note that if
		// using the transactional API these are prewrites, no committed writes).
		err = seq.innerServer.Write(ctxt, txn.Writes)
		if handleError(err, task) {
			continue
		}

		// Send response back to the gRPC thread.
		task.resultChannel <- tikv.RespOk(task.cmd.Response())

		close(task.resultChannel)
	}
}

// Give the command in task an opportunity to handle err. Returns true if there was an error so the caller is done with
// this command.
func handleError(err error, task task) bool {
	if err == nil {
		return false
	}

	if resp := task.cmd.HandleError(err); resp != nil {
		task.resultChannel <- tikv.RespOk(resp)
	} else {
		task.resultChannel <- tikv.RespErr(err)
	}

	close(task.resultChannel)
	return true
}

func (seq *Sequential) Stop() {
	seq.queue <- task{nil, nil}
}

func (seq *Sequential) Run(cmd tikv.Command) <-chan tikv.RespResult {
	channel := make(chan tikv.RespResult, 1)
	tsk := task{cmd, channel}
	seq.queue <- tsk
	return channel
}

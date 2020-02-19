package exec

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/interfaces"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
)

// Sequential is a Scheduler which executes all commands sequentially on a single thread. Since there is no concurrency,
// no latching, etc. is required.
type Sequential struct {
	innerServer interfaces.InnerServer
	queue       chan task
}

// task is a task to be run by a scheduler.
type task struct {
	cmd           interfaces.Command
	resultChannel chan<- interfaces.SchedResult
}

// NewSeqScheduler creates a new sequential scheduler.
func NewSeqScheduler(innerServer interfaces.InnerServer) *Sequential {
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
		reader, err := seq.innerServer.Reader(ctxt)
		if handleError(err, task, nil) {
			continue
		}

		// Build an mvcc transaction.
		txn := kvstore.NewTxn(reader)
		err = task.cmd.BuildTxn(&txn)
		if handleError(err, task, reader) {
			continue
		}

		// Building the transaction succeeded without conflict, write all its writes to backing storage (note that if
		// using the transactional API these are prewrites, no committed writes).
		err = seq.innerServer.Write(ctxt, txn.Writes)
		if handleError(err, task, reader) {
			continue
		}

		// Send response back to the gRPC thread.
		task.resultChannel <- interfaces.RespOk(task.cmd.Response())

		reader.Close()
		close(task.resultChannel)
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

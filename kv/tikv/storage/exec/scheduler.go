package exec

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/interfaces"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
)

// Sequential is a Scheduler which executes all commands sequentially on a single thread. Since there is no concurrency,
// no latching, etc. is required.
type Sequential struct {
	innerServer interfaces.InnerServer
	queue       chan task
	Latches     *latches
}

// task is a task to be run by a scheduler.
type task struct {
	cmd           interfaces.Command
	resultChannel chan<- interfaces.SchedResult
}

// NewSeqScheduler creates a new sequential scheduler.
func NewSeqScheduler(innerServer interfaces.InnerServer) *Sequential {
	sched := &Sequential{innerServer: innerServer, queue: make(chan task), Latches: newLatches()}
	go sched.listenTasks()
	return sched
}

// handleTask takes a task from Sequential's queue and executes it.
func (seq *Sequential) listenTasks() {
	for {
		task := <-seq.queue

		// A nil task is a command to stop and close down the scheduler.
		if task.cmd == nil && task.resultChannel == nil {
			close(seq.queue)
			return
		}

		err := seq.handleTask(task)
		if err != nil {
			task.resultChannel <- interfaces.RespErr(err)
		}
		close(task.resultChannel)
	}
}

func (seq *Sequential) handleTask(task task) error {
	ctxt := task.cmd.Context()
	var resp interface{}

	latches := task.cmd.WillWrite()
	if latches == nil {
		// The command is readonly or requires access to the DB to determine the keys it will write.
		reader, err := seq.innerServer.Reader(ctxt)
		if err != nil {
			return err
		}
		txn := kvstore.RoTxn{Reader: reader}
		resp, latches, err = task.cmd.Read(&txn)
		reader.Close()
		if err != nil {
			return err
		}
	}

	if latches != nil {
		// The command will write to the DB.
		for {
			wg := seq.Latches.acquireLatches(latches)
			if wg == nil {
				break
			}
			wg.Wait()
		}
		defer seq.Latches.releaseLatches(latches)

		reader, err := seq.innerServer.Reader(ctxt)
		if err != nil {
			return err
		}
		defer reader.Close()

		// Build an mvcc transaction.
		txn := kvstore.NewTxn(reader)
		resp, err = task.cmd.PrepareWrites(&txn)
		if err != nil {
			return err
		}

		seq.Latches.validate(&txn, latches)

		// Building the transaction succeeded without conflict, write all writes to backing storage.
		err = seq.innerServer.Write(ctxt, txn.Writes)
		if err != nil {
			return err
		}
	}

	// Send response back to the gRPC thread.
	task.resultChannel <- interfaces.RespOk(resp)
	return nil
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

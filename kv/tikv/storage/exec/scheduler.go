package exec

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/tikv"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
)

// Sequential is a Scheduler which executes all commands sequentially on a single thread. Since there is no concurrency,
// no latching, etc. is required.
type Sequential struct {
	innerServer tikv.InnerServer
	queue       chan task
}

type task struct {
	cmd           tikv.Command
	resultChannel chan<- tikv.RespResult
}

func NewSeqScheduler(innerServer tikv.InnerServer) *Sequential {
	sched := &Sequential{innerServer, make(chan task)}
	go sched.handleTask()
	return sched
}

func (seq *Sequential) handleTask() {
	for {
		task := <-seq.queue

		if task.cmd == nil && task.resultChannel == nil {
			close(seq.queue)
			return
		}

		ctxt := task.cmd.Context()
		reader, err := seq.innerServer.Reader(ctxt)
		if err != nil {
			if regResp := task.cmd.RegionError(tikv.ExtractRegionError(err)); regResp != nil {
				task.resultChannel <- tikv.RespOk(regResp)
			} else {
				task.resultChannel <- tikv.RespErr(err)
			}
		}

		txn := kvstore.NewTxn(reader)
		err = task.cmd.BuildTxn(&txn)
		if err != nil {
			task.resultChannel <- tikv.RespErr(err)
		}

		fmt.Printf("writes: %+v", txn.Writes)
		err = seq.innerServer.Write(ctxt, txn.Writes)
		if err != nil {
			task.resultChannel <- tikv.RespErr(err)
		}

		result, err := task.cmd.Response()
		if err != nil {
			task.resultChannel <- tikv.RespErr(err)
		}

		task.resultChannel <- tikv.RespOk(result)

		close(task.resultChannel)
	}
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

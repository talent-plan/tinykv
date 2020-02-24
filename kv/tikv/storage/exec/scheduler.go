package exec

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/interfaces"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
)

type Scheduler struct {
	innerServer interfaces.InnerServer
	Latches     *latches
}

// NewScheduler creates a new sequential scheduler.
func NewScheduler(innerServer interfaces.InnerServer) *Scheduler {
	return &Scheduler{innerServer, newLatches()}
}

func (seq *Scheduler) Run(cmd interfaces.Command) (interface{}, error) {
	ctxt := cmd.Context()
	var resp interface{}

	latches := cmd.WillWrite()
	if latches == nil {
		// The command is readonly or requires access to the DB to determine the keys it will write.
		reader, err := seq.innerServer.Reader(ctxt)
		if err != nil {
			return nil, err
		}
		txn := kvstore.RoTxn{Reader: reader}
		resp, latches, err = cmd.Read(&txn)
		reader.Close()
		if err != nil {
			return nil, err
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
			return nil, err
		}
		defer reader.Close()

		// Build an mvcc transaction.
		txn := kvstore.NewTxn(reader)
		resp, err = cmd.PrepareWrites(&txn)
		if err != nil {
			return nil, err
		}

		seq.Latches.validate(&txn, latches)

		// Building the transaction succeeded without conflict, write all writes to backing storage.
		err = seq.innerServer.Write(ctxt, txn.Writes)
		if err != nil {
			return nil, err
		}
	}

	return resp, nil
}

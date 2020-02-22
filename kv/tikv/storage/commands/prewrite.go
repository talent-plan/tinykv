package commands

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// Prewrite represents the prewrite stage of a transaction. A prewrite contains all writes (but not reads) in a transaction,
// if the whole transaction can be written to underlying storage atomically and without conflicting with other
// transactions (complete or in-progress) then success is returned to the client. If all a client's prewrites succeed,
// then it will send a commit message. I.e., prewrite is the first phase in a two phase commit.
type Prewrite struct {
	CommandBase
	request *kvrpcpb.PrewriteRequest
}

func NewPrewrite(request *kvrpcpb.PrewriteRequest) Prewrite {
	return Prewrite{
		CommandBase: CommandBase{
			context: request.Context,
		},
		request: request,
	}
}

func (p *Prewrite) Execute(txn *kvstore.MvccTxn) (interface{}, error) {
	response := new(kvrpcpb.PrewriteResponse)
	txn.StartTS = &p.request.StartVersion

	// Prewrite all mutations in the request.
	for _, m := range p.request.Mutations {
		keyError, err := p.prewriteMutation(txn, m)
		if keyError != nil {
			response.Errors = append(response.Errors, keyError)
		} else if err != nil {
			return regionError(err, response)
		}
	}

	return response, nil
}

// prewriteMutation prewrites mut to txn. It returns (nil, nil) on success, (err, nil) if the key in mut is already
// locked or there is any other key error, and (nil, err) if an internal error occurs.
func (p *Prewrite) prewriteMutation(txn *kvstore.MvccTxn, mut *kvrpcpb.Mutation) (*kvrpcpb.KeyError, error) {
	key := mut.Key
	// Check for write conflicts.
	if write, writeCommitTS, err := txn.SeekWrite(key, kvstore.TsMax); write != nil && err == nil {
		if writeCommitTS >= *txn.StartTS {
			keyError := new(kvrpcpb.KeyError)
			keyError.Conflict = &kvrpcpb.WriteConflict{
				StartTs:    *txn.StartTS,
				ConflictTs: write.StartTS,
				Key:        key,
				Primary:    p.request.PrimaryLock,
			}
			return keyError, nil
		}
	} else if err != nil {
		return nil, err
	}

	// Check if key is locked.
	if existingLock, err := txn.GetLock(key); err != nil {
		return nil, err
	} else if existingLock != nil {
		if existingLock.Ts != *txn.StartTS {
			// Key is locked by someone else.
			keyError := new(kvrpcpb.KeyError)
			keyError.Locked = existingLock.Info(key)
			return keyError, nil
		} else {
			// Key is locked by us
			return nil, nil
		}
	}

	// Write a lock and value.
	lock := kvstore.Lock{
		Primary: p.request.PrimaryLock,
		Ts:      *txn.StartTS,
		Kind:    kvstore.WriteKindFromProto(mut.Op),
		Ttl:     p.request.LockTtl,
	}
	txn.PutLock(key, &lock)
	txn.PutValue(key, mut.Value)

	return nil, nil
}

func (p *Prewrite) WillWrite(reader dbreader.DBReader) ([][]byte, error) {
	var result [][]byte
	for _, m := range p.request.Mutations {
		result = append(result, m.Key)
	}
	return result, nil
}

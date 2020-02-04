package commands

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// Prewrite represents the prewrite stage of a transaction. A prewrite contains all writes (but not reads) in a transaction,
// if the whole transaction can be written to underlying storage atomically and without conflicting with other
// transactions (complete or in-progress) then success is returned to the client. If all a client's prewrites succeed,
// then it will send a commit message. I.e., prewrite is the first phase in a two phase commit.
type Prewrite struct {
	request *kvrpcpb.PrewriteRequest
}

func NewPrewrite(request *kvrpcpb.PrewriteRequest) Prewrite {
	return Prewrite{request}
}

func (p *Prewrite) BuildTxn(txn *kvstore.MvccTxn) error {
	txn.StartTS = &p.request.StartVersion

	var locks []kvrpcpb.LockInfo
	// Prewrite all mutations in the request.
	for _, m := range p.request.Mutations {
		lockErr, err := p.prewriteMutation(txn, m)
		if lockErr != nil {
			locks = append(locks, *lockErr)
		} else if err != nil {
			return err
		}
	}

	// If no keys were locked, then the prewrite succeeded.
	if len(locks) == 0 {
		return nil
	}

	// If any keys were locked, we'll return this to the client.
	return &LockedError{locks}
}

func (p *Prewrite) Context() *kvrpcpb.Context {
	return p.request.Context
}

func (p *Prewrite) Response() interface{} {
	return &kvrpcpb.PrewriteResponse{}
}

func (p *Prewrite) HandleError(err error) interface{} {
	if err == nil {
		return nil
	}

	if regionErr := extractRegionError(err); regionErr != nil {
		resp := kvrpcpb.PrewriteResponse{}
		resp.RegionError = regionErr
		return &resp
	}

	if e, ok := err.(KeyError); ok {
		resp := kvrpcpb.PrewriteResponse{}
		resp.Errors = e.keyErrors()
		return &resp
	}

	return nil
}

// prewriteMutation prewrites mut to txn. It returns (nil, nil) on success, (lock, nil) if the key in mut is already
// locked, and (nil, err) if an error occurs.
func (p *Prewrite) prewriteMutation(txn *kvstore.MvccTxn, mut *kvrpcpb.Mutation) (*kvrpcpb.LockInfo, error) {
	key := mut.Key
	// Check for write conflicts.
	if write, writeCommitTS, err := txn.SeekWrite(key); write != nil && err == nil {
		if writeCommitTS >= *txn.StartTS {
			return nil, &WriteConflict{
				startTS:          *txn.StartTS,
				conflictTS:       write.StartTS,
				key:              key,
				primary:          p.request.PrimaryLock,
				conflictCommitTS: writeCommitTS,
			}
		}
		// Check data constraint - insert should not exist.
		if mut.Op == kvrpcpb.Op_Insert {
			switch write.Kind {
			case kvstore.WriteKindPut:
				return nil, &AlreadyExist{key}
			case kvstore.WriteKindLock, kvstore.WriteKindRollback:
				if write, err := txn.GetWrite(key, writeCommitTS-1); write != nil {
					return nil, &AlreadyExist{key}
				} else if err != nil {
					return nil, err
				}
			}
		}
	} else if err != nil {
		return nil, err
	}

	// Check if key is locked.
	if existingLock, err := txn.GetLock(key); err != nil {
		return nil, err
	} else if existingLock != nil {
		if existingLock.TS != *txn.StartTS {
			// Key is locked by someone else.
			return existingLock.Info(key), nil
		} else {
			// Key is locked by us
			return nil, nil
		}
	}

	// Write a lock and value.
	lock := kvstore.Lock{
		Primary: p.request.PrimaryLock,
		TS:      *txn.StartTS,
	}
	txn.PutLock(key, &lock)
	txn.PutValue(key, mut.Value)

	return nil, nil
}

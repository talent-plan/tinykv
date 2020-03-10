package standalone_server

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/errors"
)

// StandAloneInnerServer is an InnerServer for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneInnerServer struct {
	db *badger.DB
}

func NewStandAloneInnerServer(conf *config.Config) *StandAloneInnerServer {
	db := engine_util.CreateDB("kv", conf)
	return &StandAloneInnerServer{
		db: db,
	}
}

func (is *StandAloneInnerServer) Start() error {
	return nil
}

func (is *StandAloneInnerServer) Stop() error {
	return is.db.Close()
}

func (is *StandAloneInnerServer) Reader(ctx *kvrpcpb.Context) (storage.DBReader, error) {
	txn := is.db.NewTransaction(false)
	reader := NewBadgerReader(txn)
	return reader, nil
}

func (is *StandAloneInnerServer) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	return is.db.Update(func(txn *badger.Txn) error {
		for _, op := range batch {
			var err error
			switch op.Type {
			case storage.ModifyTypePut:
				put := op.Data.(storage.Put)
				err = txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value)
			case storage.ModifyTypeDelete:
				delete := op.Data.(storage.Delete)
				err = txn.Delete(engine_util.KeyWithCF(delete.Cf, delete.Key))
			default:
				err = errors.New("Unsupported modify type")
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
}

type BadgerReader struct {
	txn *badger.Txn
}

func NewBadgerReader(txn *badger.Txn) *BadgerReader {
	return &BadgerReader{txn}
}

func (b *BadgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCFFromTxn(b.txn, cf, key)
}

func (b *BadgerReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, b.txn)
}

func (b *BadgerReader) Close() {
	b.txn.Discard()
}

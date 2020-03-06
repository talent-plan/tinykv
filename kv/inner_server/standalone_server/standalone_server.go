package standalone_server

import (
	"github.com/Connor1996/badger"
	kvConfig "github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/inner_server"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/errors"
)

// StandAloneInnerServer is an InnerServer for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneInnerServer struct {
	db *badger.DB
}

func NewStandAloneInnerServer(conf *kvConfig.Config) *StandAloneInnerServer {
	db := engine_util.CreateDB("kv", conf)
	return &StandAloneInnerServer{
		db: db,
	}
}

func (is *StandAloneInnerServer) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return nil
}

func (is *StandAloneInnerServer) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return nil
}

func (is *StandAloneInnerServer) Start(pdClient pd.Client) error {
	return nil
}

func (is *StandAloneInnerServer) Stop() error {
	return is.db.Close()
}

func (is *StandAloneInnerServer) Reader(ctx *kvrpcpb.Context) (inner_server.DBReader, error) {
	txn := is.db.NewTransaction(false)
	reader := NewBadgerReader(txn)
	return reader, nil
}

func (is *StandAloneInnerServer) Write(ctx *kvrpcpb.Context, batch []inner_server.Modify) error {
	return is.db.Update(func(txn *badger.Txn) error {
		for _, op := range batch {
			var err error
			switch op.Type {
			case inner_server.ModifyTypePut:
				put := op.Data.(inner_server.Put)
				err = txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value)
			case inner_server.ModifyTypeDelete:
				delete := op.Data.(inner_server.Delete)
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

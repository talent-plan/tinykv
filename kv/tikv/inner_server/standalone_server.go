package inner_server

import (
	"github.com/coocood/badger"
	kvConfig "github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tikvpb"
	"github.com/pingcap/errors"
)

// StandAloneInnerServer is an InnerServer (see tikv/server.go) for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneInnerServer struct {
	db *badger.DB
}

func NewStandAloneInnerServer(conf *kvConfig.Config) *StandAloneInnerServer {
	db := engine_util.CreateDB("kv", &conf.Engine)
	return &StandAloneInnerServer{
		db: db,
	}
}

func (is *StandAloneInnerServer) Raft(stream tikvpb.Tikv_RaftServer) error {
	return nil
}

func (is *StandAloneInnerServer) Snapshot(stream tikvpb.Tikv_SnapshotServer) error {
	return nil
}

func (is *StandAloneInnerServer) Start(pdClient pd.Client) error {
	return nil
}

func (is *StandAloneInnerServer) Stop() error {
	return is.db.Close()
}

func (is *StandAloneInnerServer) Reader(ctx *kvrpcpb.Context) (dbreader.DBReader, error) {
	txn := is.db.NewTransaction(false)
	reader := dbreader.NewBadgerReader(txn)
	return reader, nil
}

func (is *StandAloneInnerServer) Write(ctx *kvrpcpb.Context, batch []Modify) error {
	return is.db.Update(func(txn *badger.Txn) error {
		for _, op := range batch {
			var err error
			switch op.Type {
			case ModifyTypePut:
				put := op.Data.(Put)
				err = txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value)
			case ModifyTypeDelete:
				delete := op.Data.(Delete)
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

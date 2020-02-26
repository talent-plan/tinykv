package storage

import (
	"context"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/pd"
	"github.com/pingcap-incubator/tinykv/kv/tikv/dbreader"
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/latches"
	"github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tikvpb"
)

// InnerServer represents the internal-facing server part of TinyKV, it handles sending and receiving from other
// TinyKV nodes. As part of that responsibility, it also reads and writes data to disk (or semi-permanent memory).
type InnerServer interface {
	Start(pdClient pd.Client) error
	Stop() error
	Write(ctx *kvrpcpb.Context, batch []inner_server.Modify) error
	Reader(ctx *kvrpcpb.Context) (dbreader.DBReader, error)
	Raft(stream tikvpb.Tikv_RaftServer) error
	Snapshot(stream tikvpb.Tikv_SnapshotServer) error
}

var _ tikvpb.TikvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	innerServer InnerServer
	Latches     *latches.Latches
}

func NewServer(innerServer InnerServer) *Server {
	return &Server{
		innerServer: innerServer,
		Latches:     latches.NewLatches(),
	}
}

// Run runs a transactional command.
func (server *Server) Run(cmd Command) (interface{}, error) {
	ctxt := cmd.Context()
	var resp interface{}

	latches := cmd.WillWrite()
	if latches == nil {
		// The command is readonly or requires access to the DB to determine the keys it will write.
		reader, err := server.innerServer.Reader(ctxt)
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

		server.Latches.WaitForLatches(latches)
		defer server.Latches.ReleaseLatches(latches)

		reader, err := server.innerServer.Reader(ctxt)
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

		server.Latches.Validate(&txn, latches)

		// Building the transaction succeeded without conflict, write all writes to backing storage.
		err = server.innerServer.Write(ctxt, txn.Writes)
		if err != nil {
			return nil, err
		}
	}

	return resp, nil
}

// The below functions are Server's gRPC API (implements TikvServer).

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	cmd := NewGet(req)
	resp, err := server.Run(&cmd)
	return resp.(*kvrpcpb.GetResponse), err
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	cmd := NewScan(req)
	resp, err := server.Run(&cmd)
	return resp.(*kvrpcpb.ScanResponse), err
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	cmd := NewPrewrite(req)
	resp, err := server.Run(&cmd)
	return resp.(*kvrpcpb.PrewriteResponse), err
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	cmd := NewCommit(req)
	resp, err := server.Run(&cmd)
	return resp.(*kvrpcpb.CommitResponse), err
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	cmd := NewCheckTxnStatus(req)
	resp, err := server.Run(&cmd)
	return resp.(*kvrpcpb.CheckTxnStatusResponse), err
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	cmd := NewRollback(req)
	resp, err := server.Run(&cmd)
	return resp.(*kvrpcpb.BatchRollbackResponse), err
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	cmd := NewResolveLock(req)
	resp, err := server.Run(&cmd)
	return resp.(*kvrpcpb.ResolveLockResponse), err
}

// Raw API. These commands are handled inline rather than by using Run and am implementation of the Commands interface.
// This is because these commands are fairly straightforward and do not share a lot of code with the transactional
// commands.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	response := new(kvrpcpb.RawGetResponse)
	reader, err := server.innerServer.Reader(req.Context)
	if err != nil {
		response.Error = err.Error()
	} else {
		val, err := reader.GetCF(req.Cf, req.Key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				response.NotFound = true
			} else {
				rawRegionError(err, response)
			}
		} else {
			response.Value = val
		}
	}

	return response, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	server.innerServer.Write(req.Context, []inner_server.Modify{{
		Type: inner_server.ModifyTypePut,
		Data: inner_server.Put{
			Key:   req.Key,
			Value: req.Value,
			Cf:    req.Cf,
		}}})
	return new(kvrpcpb.RawPutResponse), nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	server.innerServer.Write(req.Context, []inner_server.Modify{{
		Type: inner_server.ModifyTypeDelete,
		Data: inner_server.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		}}})
	return new(kvrpcpb.RawDeleteResponse), nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	response := new(kvrpcpb.RawScanResponse)

	reader, err := server.innerServer.Reader(req.Context)
	if err != nil {
		response.Error = err.Error()
	} else {
		it := reader.IterCF(req.Cf)
		defer it.Close()
		for it.Seek(req.StartKey); it.Valid() && len(response.Kvs) < int(req.Limit); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			value, err := item.ValueCopy(nil)
			if err != nil {
				rawRegionError(err, response)
				break
			} else {
				response.Kvs = append(response.Kvs, &kvrpcpb.KvPair{
					Key:   key,
					Value: value,
				})
			}
		}
	}

	return response, nil
}

// Raft commands (tikv <-> tikv); these are trivially forwarded to innerServer.
func (server *Server) Raft(stream tikvpb.Tikv_RaftServer) error {
	return server.innerServer.Raft(stream)
}

func (server *Server) Snapshot(stream tikvpb.Tikv_SnapshotServer) error {
	return server.innerServer.Snapshot(stream)
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coprocessor.Request) (*coprocessor.Response, error) {
	return &coprocessor.Response{}, nil
}

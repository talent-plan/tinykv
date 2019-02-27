package tikv

import (
	"context"
	"io"
	"sync"

	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/tikvpb"
)

const responseBufferSize = 128

type batchRequestHandler struct {
	responseBuffer struct {
		sync.Mutex
		resps []*tikvpb.BatchCommandsResponse_Response
		ids   []uint64
	}
	done   chan struct{}
	wakeUp chan struct{}

	svr    *Server
	stream tikvpb.Tikv_BatchCommandsServer
}

func (svr *Server) BatchCommands(stream tikvpb.Tikv_BatchCommandsServer) error {
	h := &batchRequestHandler{
		done:   make(chan struct{}),
		wakeUp: make(chan struct{}, 1),
		svr:    svr,
		stream: stream,
	}
	h.responseBuffer.resps = make([]*tikvpb.BatchCommandsResponse_Response, 0, responseBufferSize)
	h.responseBuffer.ids = make([]uint64, 0, responseBufferSize)

	return h.start()
}

func (h *batchRequestHandler) start() error {
	ctx, cancel := context.WithCancel(h.stream.Context())
	go func() {
		if err := h.dispatchBatchRequest(ctx); err != nil {
			log.Warn(err)
		}
		close(h.done)
	}()

	err := h.collectAndSendResponse()
	cancel()
	return err
}

func (h *batchRequestHandler) handleRequest(id uint64, req *tikvpb.BatchCommandsRequest_Request) {
	resp, err := h.svr.handleBatchRequest(h.stream.Context(), req)
	if err != nil {
		log.Warn(err)
		return
	}
	h.responseBuffer.Lock()
	h.responseBuffer.ids = append(h.responseBuffer.ids, id)
	h.responseBuffer.resps = append(h.responseBuffer.resps, resp)
	h.responseBuffer.Unlock()
	select {
	case h.wakeUp <- struct{}{}:
	default:
	}
}

func (h *batchRequestHandler) dispatchBatchRequest(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		batchReq, err := h.stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		for i, req := range batchReq.GetRequests() {
			id := batchReq.GetRequestIds()[i]
			go h.handleRequest(id, req)
		}
	}
}

func (h *batchRequestHandler) collectAndSendResponse() error {
	batchResp := &tikvpb.BatchCommandsResponse{
		Responses:  make([]*tikvpb.BatchCommandsResponse_Response, 0, responseBufferSize),
		RequestIds: make([]uint64, 0, responseBufferSize),
	}

	var done bool
	for {
		select {
		case <-h.wakeUp:
		case <-h.done:
			done = true
		}

		h.responseBuffer.Lock()
		if len(h.responseBuffer.ids) == 0 {
			h.responseBuffer.Unlock()
			if done {
				return nil
			}
			continue
		}
		h.responseBuffer.ids, batchResp.RequestIds = batchResp.RequestIds, h.responseBuffer.ids
		h.responseBuffer.resps, batchResp.Responses = batchResp.Responses, h.responseBuffer.resps
		h.responseBuffer.Unlock()

		if err := h.stream.Send(batchResp); err != nil {
			return err
		}
		batchResp.Responses = batchResp.Responses[:0]
		batchResp.RequestIds = batchResp.RequestIds[:0]
	}
}

func (svr *Server) handleBatchRequest(ctx context.Context, req *tikvpb.BatchCommandsRequest_Request) (*tikvpb.BatchCommandsResponse_Response, error) {
	switch req := req.GetCmd().(type) {
	case *tikvpb.BatchCommandsRequest_Request_Get:
		res, err := svr.KvGet(ctx, req.Get)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_Get{Get: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_Scan:
		res, err := svr.KvScan(ctx, req.Scan)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_Scan{Scan: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_Prewrite:
		res, err := svr.KvPrewrite(ctx, req.Prewrite)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_Prewrite{Prewrite: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_Commit:
		res, err := svr.KvCommit(ctx, req.Commit)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_Commit{Commit: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_Cleanup:
		res, err := svr.KvCleanup(ctx, req.Cleanup)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_Cleanup{Cleanup: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_BatchGet:
		res, err := svr.KvBatchGet(ctx, req.BatchGet)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_BatchGet{BatchGet: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_BatchRollback:
		res, err := svr.KvBatchRollback(ctx, req.BatchRollback)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_BatchRollback{BatchRollback: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_ScanLock:
		res, err := svr.KvScanLock(ctx, req.ScanLock)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_ScanLock{ScanLock: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_ResolveLock:
		res, err := svr.KvResolveLock(ctx, req.ResolveLock)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_ResolveLock{ResolveLock: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_GC:
		res, err := svr.KvGC(ctx, req.GC)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_GC{GC: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_DeleteRange:
		res, err := svr.KvDeleteRange(ctx, req.DeleteRange)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_DeleteRange{DeleteRange: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_RawGet:
		res, err := svr.RawGet(ctx, req.RawGet)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_RawGet{RawGet: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_RawBatchGet:
		res, err := svr.RawBatchGet(ctx, req.RawBatchGet)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_RawBatchGet{RawBatchGet: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_RawPut:
		res, err := svr.RawPut(ctx, req.RawPut)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_RawPut{RawPut: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_RawBatchPut:
		res, err := svr.RawBatchPut(ctx, req.RawBatchPut)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_RawBatchPut{RawBatchPut: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_RawDelete:
		res, err := svr.RawDelete(ctx, req.RawDelete)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_RawDelete{RawDelete: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_RawBatchDelete:
		res, err := svr.RawBatchDelete(ctx, req.RawBatchDelete)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_RawBatchDelete{RawBatchDelete: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_RawScan:
		res, err := svr.RawScan(ctx, req.RawScan)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_RawScan{RawScan: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_RawDeleteRange:
		res, err := svr.RawDeleteRange(ctx, req.RawDeleteRange)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_RawDeleteRange{RawDeleteRange: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_Coprocessor:
		res, err := svr.Coprocessor(ctx, req.Coprocessor)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_Coprocessor{Coprocessor: res}}, nil
	}
	return nil, nil
}

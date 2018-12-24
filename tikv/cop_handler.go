package tikv

import (
	"bytes"
	"io"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/unistore/rowcodec"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	mockpkg "github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var dummySlice = make([]byte, 0)

type dagContext struct {
	reqCtx    *requestCtx
	dagReq    *tipb.DAGRequest
	keyRanges []*coprocessor.KeyRange
	evalCtx   *evalContext
}

func (svr *Server) handleCopDAGRequest(reqCtx *requestCtx, req *coprocessor.Request) *coprocessor.Response {
	startTime := time.Now()
	resp := &coprocessor.Response{}
	dagCtx, dagReq, err := svr.buildDAG(reqCtx, req)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}
	var (
		chunks []tipb.Chunk
		rowCnt int
	)
	closureExec, err := svr.tryBuildClosureExecutor(dagCtx, dagReq)
	if err != nil {
		return buildResp(chunks, nil, err, dagCtx.evalCtx.sc.GetWarnings(), time.Since(startTime))
	}
	if closureExec != nil {
		chunks, err = closureExec.execute()
		return buildResp(chunks, nil, err, dagCtx.evalCtx.sc.GetWarnings(), time.Since(startTime))
	}
	e, err := svr.buildDAGExecutor(dagCtx, dagReq.Executors)
	ctx := context.TODO()
	for {
		var row [][]byte
		row, err = e.Next(ctx)
		if err != nil {
			break
		}
		if row == nil {
			break
		}
		data := dummySlice
		for _, offset := range dagReq.OutputOffsets {
			data = append(data, row[offset]...)
		}
		chunks = appendRow(chunks, data, rowCnt)
		rowCnt++
	}
	warnings := dagCtx.evalCtx.sc.GetWarnings()
	return buildResp(chunks, e.Counts(), err, warnings, time.Since(startTime))
}

func (svr *Server) buildDAG(reqCtx *requestCtx, req *coprocessor.Request) (*dagContext, *tipb.DAGRequest, error) {
	if len(req.Ranges) == 0 {
		return nil, nil, errors.New("request range is null")
	}
	if req.GetTp() != kv.ReqTypeDAG {
		return nil, nil, errors.Errorf("unsupported request type %d", req.GetTp())
	}

	dagReq := new(tipb.DAGRequest)
	err := proto.Unmarshal(req.Data, dagReq)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	sc := flagsToStatementContext(dagReq.Flags)
	sc.TimeZone = time.FixedZone("UTC", int(dagReq.TimeZoneOffset))
	ctx := &dagContext{
		reqCtx:    reqCtx,
		dagReq:    dagReq,
		keyRanges: req.Ranges,
		evalCtx:   &evalContext{sc: sc},
	}
	scanExec := dagReq.Executors[0]
	if scanExec.Tp == tipb.ExecType_TypeTableScan {
		ctx.evalCtx.setColumnInfo(scanExec.TblScan.Columns)
	} else {
		ctx.evalCtx.setColumnInfo(scanExec.IdxScan.Columns)
	}
	return ctx, dagReq, err
}

func (svr *Server) handleCopStream(ctx context.Context, reqCtx *requestCtx, req *coprocessor.Request) (tikvpb.Tikv_CoprocessorStreamClient, error) {
	dagCtx, dagReq, err := svr.buildDAG(reqCtx, req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	e, err := svr.buildDAGExecutor(dagCtx, dagReq.Executors)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &mockCopStreamClient{
		exec: e,
		req:  dagReq,
		ctx:  ctx,
	}, nil
}

func (svr *Server) buildExec(ctx *dagContext, curr *tipb.Executor) (executor, error) {
	var currExec executor
	var err error
	switch curr.GetTp() {
	case tipb.ExecType_TypeTableScan:
		currExec, err = svr.buildTableScan(ctx, curr)
	case tipb.ExecType_TypeIndexScan:
		currExec, err = svr.buildIndexScan(ctx, curr)
	case tipb.ExecType_TypeSelection:
		currExec, err = svr.buildSelection(ctx, curr)
	case tipb.ExecType_TypeAggregation:
		currExec, err = svr.buildHashAgg(ctx, curr)
	case tipb.ExecType_TypeStreamAgg:
		currExec, err = svr.buildStreamAgg(ctx, curr)
	case tipb.ExecType_TypeTopN:
		currExec, err = svr.buildTopN(ctx, curr)
	case tipb.ExecType_TypeLimit:
		currExec = &limitExec{limit: curr.Limit.GetLimit()}
	default:
		// TODO: Support other types.
		err = errors.Errorf("this exec type %v doesn't support yet.", curr.GetTp())
	}

	return currExec, errors.Trace(err)
}

func (svr *Server) buildDAGExecutor(ctx *dagContext, executors []*tipb.Executor) (executor, error) {
	var src executor
	for i := 0; i < len(executors); i++ {
		curr, err := svr.buildExec(ctx, executors[i])
		if err != nil {
			return nil, errors.Trace(err)
		}
		curr.SetSrcExec(src)
		src = curr
	}
	return src, nil
}

func (svr *Server) buildTableScan(ctx *dagContext, executor *tipb.Executor) (*tableScanExec, error) {
	ranges, err := svr.extractKVRanges(ctx.reqCtx.regCtx, ctx.keyRanges, executor.TblScan.Desc)
	if err != nil {
		return nil, errors.Trace(err)
	}

	e := &tableScanExec{
		TableScan: executor.TblScan,
		kvRanges:  ranges,
		colIDs:    ctx.evalCtx.colIDs,
		startTS:   ctx.dagReq.GetStartTs(),
		mvccStore: svr.mvccStore,
		reqCtx:    ctx.reqCtx,
	}
	if ctx.dagReq.CollectRangeCounts != nil && *ctx.dagReq.CollectRangeCounts {
		e.counts = make([]int64, len(ranges))
	}
	return e, nil
}

func (svr *Server) buildIndexScan(ctx *dagContext, executor *tipb.Executor) (*indexScanExec, error) {
	var err error
	columns := ctx.evalCtx.columnInfos
	length := len(columns)
	pkStatus := pkColNotExists
	// The PKHandle column info has been collected in ctx.
	if columns[length-1].GetPkHandle() {
		if mysql.HasUnsignedFlag(uint(columns[length-1].GetFlag())) {
			pkStatus = pkColIsUnsigned
		} else {
			pkStatus = pkColIsSigned
		}
		columns = columns[:length-1]
	} else if columns[length-1].ColumnId == model.ExtraHandleID {
		pkStatus = pkColIsSigned
		columns = columns[:length-1]
	}
	ranges, err := svr.extractKVRanges(ctx.reqCtx.regCtx, ctx.keyRanges, executor.IdxScan.Desc)
	if err != nil {
		return nil, errors.Trace(err)
	}

	e := &indexScanExec{
		IndexScan: executor.IdxScan,
		kvRanges:  ranges,
		colsLen:   len(columns),
		startTS:   ctx.dagReq.GetStartTs(),
		mvccStore: svr.mvccStore,
		reqCtx:    ctx.reqCtx,
		pkStatus:  pkStatus,
		tps:       ctx.evalCtx.fieldTps,
		loc:       ctx.evalCtx.sc.TimeZone,
	}
	if ctx.dagReq.CollectRangeCounts != nil && *ctx.dagReq.CollectRangeCounts {
		e.counts = make([]int64, len(ranges))
	}
	return e, nil
}

func (svr *Server) buildSelection(ctx *dagContext, executor *tipb.Executor) (*selectionExec, error) {
	var err error
	var relatedColOffsets []int
	pbConds := executor.Selection.Conditions
	for _, cond := range pbConds {
		relatedColOffsets, err = extractOffsetsInExpr(cond, ctx.evalCtx.columnInfos, relatedColOffsets)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	conds, err := convertToExprs(ctx.evalCtx.sc, ctx.evalCtx.fieldTps, pbConds)
	if err != nil {
		return nil, errors.Trace(err)
	}
	seCtx := mockpkg.NewContext()
	seCtx.GetSessionVars().StmtCtx = ctx.evalCtx.sc
	return &selectionExec{
		evalCtx:           ctx.evalCtx,
		relatedColOffsets: relatedColOffsets,
		conditions:        conds,
		row:               make([]types.Datum, len(ctx.evalCtx.columnInfos)),
		seCtx:             seCtx,
	}, nil
}

func (svr *Server) getAggInfo(ctx *dagContext, pbAgg *tipb.Aggregation) ([]aggregation.Aggregation, []expression.Expression, []int, error) {
	length := len(pbAgg.AggFunc)
	aggs := make([]aggregation.Aggregation, 0, length)
	var err error
	var relatedColOffsets []int
	for _, expr := range pbAgg.AggFunc {
		var aggExpr aggregation.Aggregation
		aggExpr, err = aggregation.NewDistAggFunc(expr, ctx.evalCtx.fieldTps, ctx.evalCtx.sc)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		aggs = append(aggs, aggExpr)
		relatedColOffsets, err = extractOffsetsInExpr(expr, ctx.evalCtx.columnInfos, relatedColOffsets)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
	}
	for _, item := range pbAgg.GroupBy {
		relatedColOffsets, err = extractOffsetsInExpr(item, ctx.evalCtx.columnInfos, relatedColOffsets)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
	}
	groupBys, err := convertToExprs(ctx.evalCtx.sc, ctx.evalCtx.fieldTps, pbAgg.GetGroupBy())
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	return aggs, groupBys, relatedColOffsets, nil
}

func (svr *Server) buildHashAgg(ctx *dagContext, executor *tipb.Executor) (*hashAggExec, error) {
	pbAgg := executor.Aggregation
	aggs, groupBys, relatedColOffsets, err := svr.getAggInfo(ctx, pbAgg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &hashAggExec{
		evalCtx:           ctx.evalCtx,
		aggExprs:          aggs,
		groupByExprs:      groupBys,
		groups:            make(map[string]struct{}),
		groupKeys:         make([][]byte, 0),
		relatedColOffsets: relatedColOffsets,
		row:               make([]types.Datum, len(ctx.evalCtx.columnInfos)),
	}, nil
}

func (svr *Server) buildStreamAgg(ctx *dagContext, executor *tipb.Executor) (*streamAggExec, error) {
	pbAgg := executor.Aggregation
	aggs, groupBys, relatedColOffsets, err := svr.getAggInfo(ctx, pbAgg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	aggCtxs := make([]*aggregation.AggEvaluateContext, len(aggs))
	for i, agg := range aggs {
		aggCtxs[i] = agg.CreateContext(ctx.evalCtx.sc)
	}
	seCtx := mockpkg.NewContext()
	seCtx.GetSessionVars().StmtCtx = ctx.evalCtx.sc
	e := &streamAggExec{
		evalCtx:           ctx.evalCtx,
		aggExprs:          aggs,
		aggCtxs:           aggCtxs,
		groupByExprs:      groupBys,
		currGroupByValues: make([][]byte, 0),
		relatedColOffsets: relatedColOffsets,
		row:               make([]types.Datum, len(ctx.evalCtx.columnInfos)),
	}
	return e, nil
}

func (svr *Server) getTopNInfo(ctx *evalContext, topN *tipb.TopN) (heap *topNHeap, relatedColOffsets []int, conds []expression.Expression, err error) {
	pbConds := make([]*tipb.Expr, len(topN.OrderBy))
	for i, item := range topN.OrderBy {
		relatedColOffsets, err = extractOffsetsInExpr(item.Expr, ctx.columnInfos, relatedColOffsets)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		pbConds[i] = item.Expr
	}
	heap = &topNHeap{
		totalCount: int(topN.Limit),
		topNSorter: topNSorter{
			orderByItems: topN.OrderBy,
			sc:           ctx.sc,
		},
	}
	if conds, err = convertToExprs(ctx.sc, ctx.fieldTps, pbConds); err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	return heap, relatedColOffsets, conds, nil
}

func (svr *Server) buildTopN(ctx *dagContext, executor *tipb.Executor) (*topNExec, error) {
	topN := executor.TopN
	heap, relatedColOffsets, conds, err := svr.getTopNInfo(ctx.evalCtx, topN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &topNExec{
		heap:              heap,
		evalCtx:           ctx.evalCtx,
		relatedColOffsets: relatedColOffsets,
		orderByExprs:      conds,
		row:               make([]types.Datum, len(ctx.evalCtx.columnInfos)),
	}, nil
}

type evalContext struct {
	colIDs      map[int64]int
	columnInfos []*tipb.ColumnInfo
	fieldTps    []*types.FieldType
	sc          *stmtctx.StatementContext
}

func (e *evalContext) setColumnInfo(cols []*tipb.ColumnInfo) {
	e.columnInfos = make([]*tipb.ColumnInfo, len(cols))
	copy(e.columnInfos, cols)

	e.colIDs = make(map[int64]int, len(e.columnInfos))
	e.fieldTps = make([]*types.FieldType, 0, len(e.columnInfos))
	for i, col := range e.columnInfos {
		ft := fieldTypeFromPBColumn(col)
		e.fieldTps = append(e.fieldTps, ft)
		e.colIDs[col.GetColumnId()] = i
	}
}

func (e *evalContext) newRowDecoder() (*rowcodec.Decoder, error) {
	colIDs := make([]int64, len(e.columnInfos))
	defaultVals := make([][]byte, len(e.columnInfos))
	var handleColID int64
	for i, colInfo := range e.columnInfos {
		colIDs[i] = colInfo.ColumnId
		defaultVals[i] = colInfo.DefaultVal
		if colInfo.PkHandle {
			handleColID = colInfo.ColumnId
		}
	}
	return rowcodec.NewDecoder(colIDs, handleColID, e.fieldTps, defaultVals, e.sc.TimeZone)
}

func (e *evalContext) newRowDecoderForOffsets(colOffsets []int) (*rowcodec.Decoder, error) {
	var (
		handleColID int64
		colIDs      = make([]int64, len(colOffsets))
		defaultVals = make([][]byte, len(colOffsets))
		fieldsTps   = make([]*types.FieldType, len(colOffsets))
	)
	for i, off := range colOffsets {
		info := e.columnInfos[off]
		colIDs[i] = info.ColumnId
		defaultVals[i] = info.DefaultVal
		fieldsTps[i] = e.fieldTps[off]
		if info.PkHandle {
			handleColID = info.ColumnId
		}
	}

	return rowcodec.NewDecoder(colIDs, handleColID, fieldsTps, defaultVals, e.sc.TimeZone)
}

// decodeRelatedColumnVals decodes data to Datum slice according to the row information.
func (e *evalContext) decodeRelatedColumnVals(relatedColOffsets []int, value [][]byte, row []types.Datum) error {
	var err error
	for _, offset := range relatedColOffsets {
		row[offset], err = decodeColumnValue(value[offset], e.fieldTps[offset], e.sc.TimeZone)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Flags are used by tipb.SelectRequest.Flags to handle execution mode, like how to handle truncate error.
const (
	// FlagIgnoreTruncate indicates if truncate error should be ignored.
	// Read-only statements should ignore truncate error, write statements should not ignore truncate error.
	FlagIgnoreTruncate uint64 = 1
	// FlagTruncateAsWarning indicates if truncate error should be returned as warning.
	// This flag only matters if FlagIgnoreTruncate is not set, in strict sql mode, truncate error should
	// be returned as error, in non-strict sql mode, truncate error should be saved as warning.
	FlagTruncateAsWarning uint64 = 1 << 1

	// FlagPadCharToFullLength indicates if sql_mode 'PAD_CHAR_TO_FULL_LENGTH' is set.
	FlagPadCharToFullLength uint64 = 1 << 2
)

// flagsToStatementContext creates a StatementContext from a `tipb.SelectRequest.Flags`.
func flagsToStatementContext(flags uint64) *stmtctx.StatementContext {
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = (flags & FlagIgnoreTruncate) > 0
	sc.TruncateAsWarning = (flags & FlagTruncateAsWarning) > 0
	sc.PadCharToFullLength = (flags & FlagPadCharToFullLength) > 0
	return sc
}

// MockGRPCClientStream is exported for testing purpose.
func MockGRPCClientStream() grpc.ClientStream {
	return mockClientStream{}
}

// mockClientStream implements grpc ClientStream interface, its methods are never called.
type mockClientStream struct{}

func (mockClientStream) Header() (metadata.MD, error) { return nil, nil }
func (mockClientStream) Trailer() metadata.MD         { return nil }
func (mockClientStream) CloseSend() error             { return nil }
func (mockClientStream) Context() context.Context     { return nil }
func (mockClientStream) SendMsg(m interface{}) error  { return nil }
func (mockClientStream) RecvMsg(m interface{}) error  { return nil }

type mockCopStreamClient struct {
	mockClientStream

	req      *tipb.DAGRequest
	exec     executor
	ctx      context.Context
	finished bool
}

type mockCopStreamErrClient struct {
	mockClientStream

	*errorpb.Error
}

func (mock *mockCopStreamErrClient) Recv() (*coprocessor.Response, error) {
	return &coprocessor.Response{
		RegionError: mock.Error,
	}, nil
}

func (mock *mockCopStreamClient) Recv() (*coprocessor.Response, error) {
	select {
	case <-mock.ctx.Done():
		return nil, mock.ctx.Err()
	default:
	}

	if mock.finished {
		return nil, io.EOF
	}

	if hook := mock.ctx.Value(mockpkg.HookKeyForTest("mockTiKVStreamRecvHook")); hook != nil {
		hook.(func(context.Context))(mock.ctx)
	}

	var resp coprocessor.Response
	counts := make([]int64, len(mock.req.Executors))
	chunk, finish, ran, counts, err := mock.readBlockFromExecutor()
	resp.Range = ran
	if err != nil {
		if locked, ok := errors.Cause(err).(*ErrLocked); ok {
			resp.Locked = &kvrpcpb.LockInfo{
				Key:         locked.Key,
				PrimaryLock: locked.Primary,
				LockVersion: locked.StartTS,
				LockTtl:     locked.TTL,
			}
		} else {
			resp.OtherError = err.Error()
		}
		return &resp, nil
	}
	if finish {
		// Just mark it, need to handle the last chunk.
		mock.finished = true
	}

	data, err := chunk.Marshal()
	if err != nil {
		resp.OtherError = err.Error()
		return &resp, nil
	}
	streamResponse := tipb.StreamResponse{
		Error: toPBError(err),
		Data:  data,
	}
	// The counts was the output count of each executor, but now it is the scan count of each range,
	// so we need a flag to tell them apart.
	if counts != nil {
		streamResponse.OutputCounts = make([]int64, 1+len(counts))
		copy(streamResponse.OutputCounts, counts)
		streamResponse.OutputCounts[len(counts)] = -1
	}
	resp.Data, err = proto.Marshal(&streamResponse)
	if err != nil {
		resp.OtherError = err.Error()
	}
	return &resp, nil
}

func (mock *mockCopStreamClient) readBlockFromExecutor() (tipb.Chunk, bool, *coprocessor.KeyRange, []int64, error) {
	var chunk tipb.Chunk
	var ran coprocessor.KeyRange
	var finish bool
	var desc bool
	mock.exec.ResetCounts()
	ran.Start, desc = mock.exec.Cursor()
	for count := 0; count < rowsPerChunk; count++ {
		row, err := mock.exec.Next(mock.ctx)
		if err != nil {
			ran.End, _ = mock.exec.Cursor()
			return chunk, false, &ran, nil, errors.Trace(err)
		}
		if row == nil {
			finish = true
			break
		}
		for _, offset := range mock.req.OutputOffsets {
			chunk.RowsData = append(chunk.RowsData, row[offset]...)
		}
	}

	ran.End, _ = mock.exec.Cursor()
	if desc {
		ran.Start, ran.End = ran.End, ran.Start
	}
	return chunk, finish, &ran, mock.exec.Counts(), nil
}

func buildResp(chunks []tipb.Chunk, counts []int64, err error, warnings []stmtctx.SQLWarn, dur time.Duration) *coprocessor.Response {
	resp := &coprocessor.Response{}
	selResp := &tipb.SelectResponse{
		Error:        toPBError(err),
		Chunks:       chunks,
		OutputCounts: counts,
	}
	if len(warnings) > 0 {
		selResp.Warnings = make([]*tipb.Error, 0, len(warnings))
		for i := range warnings {
			selResp.Warnings = append(selResp.Warnings, toPBError(warnings[i].Err))
		}
	}
	if err != nil {
		if locked, ok := errors.Cause(err).(*ErrLocked); ok {
			resp.Locked = &kvrpcpb.LockInfo{
				Key:         locked.Key,
				PrimaryLock: locked.Primary,
				LockVersion: locked.StartTS,
				LockTtl:     locked.TTL,
			}
		} else {
			resp.OtherError = err.Error()
		}
	}
	resp.ExecDetails = &kvrpcpb.ExecDetails{
		HandleTime: &kvrpcpb.HandleTime{ProcessMs: int64(dur / time.Millisecond)},
	}
	data, err := proto.Marshal(selResp)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}
	resp.Data = data
	return resp
}

func toPBError(err error) *tipb.Error {
	if err == nil {
		return nil
	}
	perr := new(tipb.Error)
	switch x := err.(type) {
	case *terror.Error:
		sqlErr := x.ToSQLError()
		perr.Code = int32(sqlErr.Code)
		perr.Msg = sqlErr.Message
	default:
		perr.Code = int32(1)
		perr.Msg = err.Error()
	}
	return perr
}

// extractKVRanges extracts kv.KeyRanges slice from a SelectRequest.
func (svr *Server) extractKVRanges(regCtx *regionCtx, keyRanges []*coprocessor.KeyRange, descScan bool) (kvRanges []kv.KeyRange, err error) {
	startKey := regCtx.rawStartKey()
	endKey := regCtx.rawEndKey()
	kvRanges = make([]kv.KeyRange, 0, len(keyRanges))
	for _, kran := range keyRanges {
		if bytes.Compare(kran.GetStart(), kran.GetEnd()) >= 0 {
			err = errors.Errorf("invalid range, start should be smaller than end: %v %v", kran.GetStart(), kran.GetEnd())
			return
		}

		upperKey := kran.GetEnd()
		if bytes.Compare(upperKey, startKey) <= 0 {
			continue
		}
		lowerKey := kran.GetStart()
		if len(endKey) != 0 && bytes.Compare(lowerKey, endKey) >= 0 {
			break
		}
		r := kv.KeyRange{
			StartKey: kv.Key(maxStartKey(lowerKey, startKey)),
			EndKey:   kv.Key(minEndKey(upperKey, endKey)),
		}
		kvRanges = append(kvRanges, r)
	}
	if descScan {
		reverseKVRanges(kvRanges)
	}
	return
}

func reverseKVRanges(kvRanges []kv.KeyRange) {
	for i := 0; i < len(kvRanges)/2; i++ {
		j := len(kvRanges) - i - 1
		kvRanges[i], kvRanges[j] = kvRanges[j], kvRanges[i]
	}
}

const rowsPerChunk = 64

func appendRow(chunks []tipb.Chunk, data []byte, rowCnt int) []tipb.Chunk {
	if rowCnt%rowsPerChunk == 0 {
		chunks = append(chunks, tipb.Chunk{})
	}
	cur := &chunks[len(chunks)-1]
	cur.RowsData = append(cur.RowsData, data...)
	return chunks
}

func maxStartKey(rangeStartKey kv.Key, regionStartKey []byte) []byte {
	if bytes.Compare([]byte(rangeStartKey), regionStartKey) > 0 {
		return []byte(rangeStartKey)
	}
	return regionStartKey
}

func minEndKey(rangeEndKey kv.Key, regionEndKey []byte) []byte {
	if len(regionEndKey) == 0 || bytes.Compare([]byte(rangeEndKey), regionEndKey) < 0 {
		return []byte(rangeEndKey)
	}
	return regionEndKey
}

func isDuplicated(offsets []int, offset int) bool {
	for _, idx := range offsets {
		if idx == offset {
			return true
		}
	}
	return false
}

func extractOffsetsInExpr(expr *tipb.Expr, columns []*tipb.ColumnInfo, collector []int) ([]int, error) {
	if expr == nil {
		return nil, nil
	}
	if expr.GetTp() == tipb.ExprType_ColumnRef {
		_, idx, err := codec.DecodeInt(expr.Val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !isDuplicated(collector, int(idx)) {
			collector = append(collector, int(idx))
		}
		return collector, nil
	}
	var err error
	for _, child := range expr.Children {
		collector, err = extractOffsetsInExpr(child, columns, collector)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return collector, nil
}

// fieldTypeFromPBColumn creates a types.FieldType from tipb.ColumnInfo.
func fieldTypeFromPBColumn(col *tipb.ColumnInfo) *types.FieldType {
	return &types.FieldType{
		Tp:      byte(col.GetTp()),
		Flag:    uint(col.Flag),
		Flen:    int(col.GetColumnLen()),
		Decimal: int(col.GetDecimal()),
		Elems:   col.Elems,
		Collate: mysql.Collations[uint8(col.GetCollation())],
	}
}

// fieldTypeFromPBFieldType creates a types.FieldType from tipb.FieldType.
func fieldTypeFromPBFieldType(ft *tipb.FieldType) *types.FieldType {
	if ft == nil {
		return nil
	}
	return &types.FieldType{
		Tp:      byte(ft.Tp),
		Flag:    uint(ft.Flag),
		Flen:    int(ft.Flen),
		Decimal: int(ft.Decimal),
		Collate: mysql.Collations[uint8(ft.Collate)],
		Charset: ft.Charset,
	}
}

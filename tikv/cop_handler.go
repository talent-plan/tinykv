package tikv

import (
	"bytes"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/unistore/rowcodec"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tipb/go-tipb"
)

var dummySlice = make([]byte, 0)

type dagContext struct {
	reqCtx    *requestCtx
	dagReq    *tipb.DAGRequest
	keyRanges []*coprocessor.KeyRange
	evalCtx   *evalContext
	startTS   uint64
}

func (svr *Server) handleCopDAGRequest(reqCtx *requestCtx, req *coprocessor.Request) *coprocessor.Response {
	startTime := time.Now()
	resp := &coprocessor.Response{}
	dagCtx, dagReq, err := svr.buildDAG(reqCtx, req)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}
	closureExec, err := svr.buildClosureExecutor(dagCtx, dagReq)
	if err != nil {
		return buildResp(nil, nil, err, dagCtx.evalCtx.sc.GetWarnings(), time.Since(startTime))
	}
	chunks, err := closureExec.execute()
	return buildResp(chunks, nil, err, dagCtx.evalCtx.sc.GetWarnings(), time.Since(startTime))
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
		startTS:   req.StartTs,
	}
	scanExec := dagReq.Executors[0]
	if scanExec.Tp == tipb.ExecType_TypeTableScan {
		ctx.evalCtx.setColumnInfo(scanExec.TblScan.Columns)
	} else {
		ctx.evalCtx.setColumnInfo(scanExec.IdxScan.Columns)
	}
	return ctx, dagReq, err
}

func (svr *Server) getAggInfo(ctx *dagContext, pbAgg *tipb.Aggregation) ([]aggregation.Aggregation, []expression.Expression, error) {
	length := len(pbAgg.AggFunc)
	aggs := make([]aggregation.Aggregation, 0, length)
	var err error
	for _, expr := range pbAgg.AggFunc {
		var aggExpr aggregation.Aggregation
		aggExpr, err = aggregation.NewDistAggFunc(expr, ctx.evalCtx.fieldTps, ctx.evalCtx.sc)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		aggs = append(aggs, aggExpr)
	}
	groupBys, err := convertToExprs(ctx.evalCtx.sc, ctx.evalCtx.fieldTps, pbAgg.GetGroupBy())
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	return aggs, groupBys, nil
}

func (svr *Server) getTopNInfo(ctx *evalContext, topN *tipb.TopN) (heap *topNHeap, conds []expression.Expression, err error) {
	pbConds := make([]*tipb.Expr, len(topN.OrderBy))
	for i, item := range topN.OrderBy {
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
		return nil, nil, errors.Trace(err)
	}

	return heap, conds, nil
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
		row[offset], err = tablecodec.DecodeColumnValue(value[offset], e.fieldTps[offset], e.sc.TimeZone)
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
	sc := &stmtctx.StatementContext{
		IgnoreTruncate:      (flags & FlagIgnoreTruncate) > 0,
		TruncateAsWarning:   (flags & FlagTruncateAsWarning) > 0,
		PadCharToFullLength: (flags & FlagPadCharToFullLength) > 0,
	}
	return sc
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

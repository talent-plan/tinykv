package coprocessor

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/juju/errors"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor/rowcodec"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	mockpkg "github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tipb/go-tipb"
)

const chunkMaxRows = 1024

const (
	pkColNotExists = iota
	pkColIsSigned
	pkColIsUnsigned
)

// buildClosureExecutor build a closureExecutor for the DAGRequest.
// Currently the composition of executors are:
// 	tableScan|indexScan [selection] [topN | limit | agg]
func (svr *CopHandler) buildClosureExecutor(dagCtx *dagContext, dagReq *tipb.DAGRequest) (*closureExecutor, error) {
	ce, err := svr.newClosureExecutor(dagCtx, dagReq)
	if err != nil {
		return nil, errors.Trace(err)
	}
	executors := dagReq.Executors
	scanExec := executors[0]
	if scanExec.Tp == tipb.ExecType_TypeTableScan {
		ce.processor = &tableScanProcessor{closureExecutor: ce}
	} else {
		ce.processor = &indexScanProcessor{closureExecutor: ce}
	}
	if len(executors) == 1 {
		return ce, nil
	}
	if secondExec := executors[1]; secondExec.Tp == tipb.ExecType_TypeSelection {
		ce.selectionCtx.conditions, err = convertToExprs(ce.sc, ce.fieldTps, secondExec.Selection.Conditions)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ce.processor = &selectionProcessor{closureExecutor: ce}
	}
	lastExecutor := executors[len(executors)-1]
	switch lastExecutor.Tp {
	case tipb.ExecType_TypeLimit:
		ce.limit = int(lastExecutor.Limit.Limit)
	case tipb.ExecType_TypeTopN:
		err = svr.buildTopNProcessor(ce, lastExecutor.TopN)
	case tipb.ExecType_TypeAggregation:
		err = svr.buildHashAggProcessor(ce, dagCtx, lastExecutor.Aggregation)
	case tipb.ExecType_TypeStreamAgg:
		err = svr.buildStreamAggProcessor(ce, dagCtx, executors)
	case tipb.ExecType_TypeSelection:
		ce.processor = &selectionProcessor{closureExecutor: ce}
	default:
		panic("unknown executor type " + lastExecutor.Tp.String())
	}
	if err != nil {
		return nil, err
	}
	return ce, nil
}

func convertToExprs(sc *stmtctx.StatementContext, fieldTps []*types.FieldType, pbExprs []*tipb.Expr) ([]expression.Expression, error) {
	exprs := make([]expression.Expression, 0, len(pbExprs))
	for _, expr := range pbExprs {
		e, err := expression.PBToExpr(expr, fieldTps, sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		exprs = append(exprs, e)
	}
	return exprs, nil
}

func (svr *CopHandler) newClosureExecutor(dagCtx *dagContext, dagReq *tipb.DAGRequest) (*closureExecutor, error) {
	e := &closureExecutor{
		evalContext: dagCtx.evalCtx,
		reader:      dagCtx.reader,
		outputOff:   dagReq.OutputOffsets,
		startTS:     dagCtx.startTS,
		limit:       math.MaxInt64,
	}
	seCtx := mockpkg.NewContext()
	seCtx.GetSessionVars().StmtCtx = e.sc
	e.seCtx = seCtx
	executors := dagReq.Executors
	scanExec := executors[0]
	switch scanExec.Tp {
	case tipb.ExecType_TypeTableScan:
		tblScan := executors[0].TblScan
		e.unique = true
		e.scanCtx.desc = tblScan.Desc
	case tipb.ExecType_TypeIndexScan:
		idxScan := executors[0].IdxScan
		e.unique = idxScan.GetUnique()
		e.scanCtx.desc = idxScan.Desc
		e.initIdxScanCtx()
	default:
		panic(fmt.Sprintf("unknown first executor type %s", executors[0].Tp))
	}
	ranges, err := svr.extractKVRanges(dagCtx.reader, dagCtx.keyRanges, e.scanCtx.desc)
	if err != nil {
		return nil, errors.Trace(err)
	}
	e.kvRanges = ranges
	e.scanCtx.chk = chunk.NewChunkWithCapacity(e.fieldTps, 32)
	if e.idxScanCtx == nil {
		e.scanCtx.decoder, err = e.evalContext.newRowDecoder()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return e, nil
}

func (e *closureExecutor) initIdxScanCtx() {
	e.idxScanCtx = new(idxScanCtx)
	e.idxScanCtx.columnLen = len(e.columnInfos)
	e.idxScanCtx.pkStatus = pkColNotExists
	lastColumn := e.columnInfos[len(e.columnInfos)-1]
	// The PKHandle column info has been collected in ctx.
	if lastColumn.GetPkHandle() {
		if mysql.HasUnsignedFlag(uint(lastColumn.GetFlag())) {
			e.idxScanCtx.pkStatus = pkColIsUnsigned
		} else {
			e.idxScanCtx.pkStatus = pkColIsSigned
		}
		e.idxScanCtx.columnLen--
	} else if lastColumn.ColumnId == model.ExtraHandleID {
		e.idxScanCtx.pkStatus = pkColIsSigned
		e.idxScanCtx.columnLen--
	}
}

func (svr *CopHandler) isCountAgg(pbAgg *tipb.Aggregation) bool {
	if len(pbAgg.AggFunc) == 1 && len(pbAgg.GroupBy) == 0 {
		aggFunc := pbAgg.AggFunc[0]
		if aggFunc.Tp == tipb.ExprType_Count && len(aggFunc.Children) == 1 {
			return true
		}
	}
	return false
}

func (svr *CopHandler) tryBuildCountProcessor(e *closureExecutor, executors []*tipb.Executor) (bool, error) {
	if len(executors) > 2 {
		return false, nil
	}
	agg := executors[1].Aggregation
	if !svr.isCountAgg(agg) {
		return false, nil
	}
	child := agg.AggFunc[0].Children[0]
	switch child.Tp {
	case tipb.ExprType_ColumnRef:
		_, idx, err := codec.DecodeInt(child.Val)
		if err != nil {
			return false, errors.Trace(err)
		}
		e.aggCtx.col = e.columnInfos[idx]
		if e.aggCtx.col.PkHandle {
			e.processor = &countStarProcessor{skipVal: skipVal(true), closureExecutor: e}
		} else {
			e.processor = &countColumnProcessor{closureExecutor: e}
		}
	case tipb.ExprType_Null, tipb.ExprType_ScalarFunc:
		return false, nil
	default:
		e.processor = &countStarProcessor{skipVal: skipVal(true), closureExecutor: e}
	}
	return true, nil
}

func (svr *CopHandler) buildTopNProcessor(e *closureExecutor, topN *tipb.TopN) error {
	heap, conds, err := svr.getTopNInfo(e.evalContext, topN)
	if err != nil {
		return errors.Trace(err)
	}

	ctx := &topNCtx{
		heap:         heap,
		orderByExprs: conds,
		sortRow:      e.newTopNSortRow(),
	}

	e.topNCtx = ctx
	e.processor = &topNProcessor{closureExecutor: e}
	return nil
}

func (svr *CopHandler) buildHashAggProcessor(e *closureExecutor, ctx *dagContext, agg *tipb.Aggregation) error {
	aggs, groupBys, err := svr.getAggInfo(ctx, agg)
	if err != nil {
		return err
	}
	e.processor = &hashAggProcessor{
		closureExecutor: e,
		aggExprs:        aggs,
		groupByExprs:    groupBys,
		groups:          map[string]struct{}{},
		groupKeys:       nil,
		aggCtxsMap:      map[string][]*aggregation.AggEvaluateContext{},
	}
	return nil
}

func (svr *CopHandler) buildStreamAggProcessor(e *closureExecutor, ctx *dagContext, executors []*tipb.Executor) error {
	ok, err := svr.tryBuildCountProcessor(e, executors)
	if err != nil || ok {
		return err
	}
	return svr.buildHashAggProcessor(e, ctx, executors[len(executors)-1].Aggregation)
}

// closureExecutor is an execution engine that flatten the DAGRequest.Executors to a single closure `processor` that
// process key/value pairs. We can define many closures for different kinds of requests, try to use the specially
// optimized one for some frequently used query.
type closureExecutor struct {
	*evalContext

	reader       storage.StorageReader
	outputOff    []uint32
	seCtx        sessionctx.Context
	kvRanges     []kv.KeyRange
	startTS      uint64
	ignoreLock   bool
	lockChecked  bool
	scanCtx      scanCtx
	idxScanCtx   *idxScanCtx
	selectionCtx selectionCtx
	aggCtx       aggCtx
	topNCtx      *topNCtx

	rowCount int
	unique   bool
	limit    int

	oldChunks []tipb.Chunk
	oldRowBuf []byte
	processor closureProcessor
}

type closureProcessor interface {
	// Process accepts key and value, should not keep reference to them.
	// Returns ScanBreak will break the scan loop.
	Process(key, value []byte) error
	// SkipValue returns if we can skip the value.
	SkipValue() bool
	Finish() error
}

type scanCtx struct {
	count   int
	limit   int
	chk     *chunk.Chunk
	desc    bool
	decoder *rowcodec.Decoder
}

type idxScanCtx struct {
	pkStatus  int
	columnLen int
}

type aggCtx struct {
	col *tipb.ColumnInfo
}

type selectionCtx struct {
	conditions []expression.Expression
}

type topNCtx struct {
	heap         *topNHeap
	orderByExprs []expression.Expression
	sortRow      *sortRow
}

func (e *closureExecutor) execute() ([]tipb.Chunk, error) {
	txn := mvcc.RoTxn{Reader: e.reader, StartTS: e.startTS}
	for _, ran := range e.kvRanges {
		if e.unique && ran.IsPoint() {
			val, err := txn.GetValue(ran.StartKey)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if len(val) == 0 {
				continue
			}
			err = e.processor.Process(ran.StartKey, val)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			if e.scanCtx.desc {
				panic("do not support desc scan")
			} else {
				scanner := mvcc.NewScanner(ran.StartKey, &txn)
				for {
					key, val, err := scanner.Next()
					if err != nil {
						scanner.Close()
						return nil, err
					}
					if key == nil && val == nil {
						break
					}
					if bytes.Compare(key, ran.EndKey) >= 0 {
						break
					}

					err = e.processor.Process(key, val)
					if err != nil {
						if err == ScanBreak {
							break
						}
						scanner.Close()
						return nil, err
					}

				}
				scanner.Close()
			}
		}
		if e.rowCount == e.limit {
			break
		}
	}
	err := e.processor.Finish()
	return e.oldChunks, err
}

type countStarProcessor struct {
	skipVal
	*closureExecutor
}

// countStarProcess is used for `count(*)`.
func (e *countStarProcessor) Process(key, value []byte) error {
	e.rowCount++
	return nil
}

func (e *countStarProcessor) Finish() error {
	return e.countFinish()
}

// countFinish is used for `count(*)`.
func (e *closureExecutor) countFinish() error {
	d := types.NewIntDatum(int64(e.rowCount))
	rowData, err := codec.EncodeValue(e.sc, nil, d)
	if err != nil {
		return errors.Trace(err)
	}
	e.oldChunks = appendRow(e.oldChunks, rowData, 0)
	return nil
}

type countColumnProcessor struct {
	skipVal
	*closureExecutor
}

func (e *countColumnProcessor) Process(key, value []byte) error {
	if e.idxScanCtx != nil {
		values, _, err := tablecodec.CutIndexKeyNew(key, e.idxScanCtx.columnLen)
		if err != nil {
			return errors.Trace(err)
		}
		if values[0][0] != codec.NilFlag {
			e.rowCount++
		}
	} else {
		// Since the handle value doesn't affect the count result, we don't need to decode the handle.
		isNull, err := e.scanCtx.decoder.ColumnIsNull(value, e.aggCtx.col.ColumnId, e.aggCtx.col.DefaultVal)
		if err != nil {
			return errors.Trace(err)
		}
		if !isNull {
			e.rowCount++
		}
	}
	return nil
}

func (e *countColumnProcessor) Finish() error {
	return e.countFinish()
}

type skipVal bool

func (s skipVal) SkipValue() bool {
	return bool(s)
}

// ScanBreak is returnd by ScanFunc to break the scan loop.
var ScanBreak = errors.New("scan break")

type tableScanProcessor struct {
	skipVal
	*closureExecutor
}

func (e *tableScanProcessor) Process(key, value []byte) error {
	if e.rowCount == e.limit {
		return ScanBreak
	}
	e.rowCount++
	err := e.tableScanProcessCore(key, value)
	if e.scanCtx.chk.NumRows() == chunkMaxRows {
		err = e.chunkToOldChunk(e.scanCtx.chk)
	}
	return err
}

func (e *tableScanProcessor) Finish() error {
	return e.scanFinish()
}

func (e *closureExecutor) processCore(key, value []byte) error {
	if e.idxScanCtx != nil {
		return e.indexScanProcessCore(key, value)
	} else {
		return e.tableScanProcessCore(key, value)
	}
}

func (e *closureExecutor) hasSelection() bool {
	return len(e.selectionCtx.conditions) > 0
}

func (e *closureExecutor) processSelection() (gotRow bool, err error) {
	chk := e.scanCtx.chk
	row := chk.GetRow(chk.NumRows() - 1)
	gotRow = true
	for _, expr := range e.selectionCtx.conditions {
		d, err := expr.Eval(row)
		if err != nil {
			return false, errors.Trace(err)
		}
		if d.IsNull() {
			gotRow = false
		} else {
			i, err := d.ToBool(e.sc)
			if err != nil {
				return false, errors.Trace(err)
			}
			gotRow = i != 0
		}
		if !gotRow {
			chk.TruncateTo(chk.NumRows() - 1)
			break
		}
	}
	return
}

func (e *closureExecutor) tableScanProcessCore(key, value []byte) error {
	handle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		return errors.Trace(err)
	}
	err = e.scanCtx.decoder.Decode(value, handle, e.scanCtx.chk)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (e *closureExecutor) scanFinish() error {
	return e.chunkToOldChunk(e.scanCtx.chk)
}

type indexScanProcessor struct {
	skipVal
	*closureExecutor
}

func (e *indexScanProcessor) Process(key, value []byte) error {
	if e.rowCount == e.limit {
		return ScanBreak
	}
	e.rowCount++
	err := e.indexScanProcessCore(key, value)
	if e.scanCtx.chk.NumRows() == chunkMaxRows {
		err = e.chunkToOldChunk(e.scanCtx.chk)
	}
	return err
}

func (e *indexScanProcessor) Finish() error {
	return e.scanFinish()
}

func (e *closureExecutor) indexScanProcessCore(key, value []byte) error {
	colLen := e.idxScanCtx.columnLen
	pkStatus := e.idxScanCtx.pkStatus
	chk := e.scanCtx.chk
	values, b, err := tablecodec.CutIndexKeyNew(key, colLen)
	if err != nil {
		return errors.Trace(err)
	}
	decoder := codec.NewDecoder(chk, e.sc.TimeZone)
	for i, colVal := range values {
		_, err = decoder.DecodeOne(colVal, i, e.fieldTps[i])
		if err != nil {
			return errors.Trace(err)
		}
	}
	if len(b) > 0 {
		if pkStatus != pkColNotExists {
			_, err = decoder.DecodeOne(b, colLen, e.fieldTps[colLen])
			if err != nil {
				return errors.Trace(err)
			}
		}
	} else if pkStatus != pkColNotExists {
		chk.AppendInt64(colLen, int64(binary.BigEndian.Uint64(value)))
	}
	return nil
}

func (e *closureExecutor) chunkToOldChunk(chk *chunk.Chunk) error {
	var oldRow []types.Datum
	for i := 0; i < chk.NumRows(); i++ {
		oldRow = oldRow[:0]
		for _, outputOff := range e.outputOff {
			d := chk.GetRow(i).GetDatum(int(outputOff), e.fieldTps[outputOff])
			oldRow = append(oldRow, d)
		}
		var err error
		e.oldRowBuf, err = codec.EncodeValue(e.sc, e.oldRowBuf[:0], oldRow...)
		if err != nil {
			return errors.Trace(err)
		}
		e.oldChunks = appendRow(e.oldChunks, e.oldRowBuf, i)
	}
	chk.Reset()
	return nil
}

type selectionProcessor struct {
	skipVal
	*closureExecutor
}

func (e *selectionProcessor) Process(key, value []byte) error {
	if e.rowCount == e.limit {
		return ScanBreak
	}
	err := e.processCore(key, value)
	if err != nil {
		return errors.Trace(err)
	}
	gotRow, err := e.processSelection()
	if err != nil {
		return err
	}
	if gotRow {
		e.rowCount++
		if e.scanCtx.chk.NumRows() == chunkMaxRows {
			err = e.chunkToOldChunk(e.scanCtx.chk)
		}
	}
	return err
}

func (e *selectionProcessor) Finish() error {
	return e.scanFinish()
}

type topNProcessor struct {
	skipVal
	*closureExecutor
}

func (e *topNProcessor) Process(key, value []byte) (err error) {
	if err = e.processCore(key, value); err != nil {
		return err
	}
	if e.hasSelection() {
		gotRow, err1 := e.processSelection()
		if err1 != nil || !gotRow {
			return err1
		}
	}

	ctx := e.topNCtx
	row := e.scanCtx.chk.GetRow(0)
	for i, expr := range ctx.orderByExprs {
		ctx.sortRow.key[i], err = expr.Eval(row)
		if err != nil {
			return errors.Trace(err)
		}
	}
	e.scanCtx.chk.Reset()

	if ctx.heap.tryToAddRow(ctx.sortRow) {
		ctx.sortRow.data[0] = append([]byte{}, key...)
		ctx.sortRow.data[1] = append([]byte{}, value...)
		ctx.sortRow = e.newTopNSortRow()
	}
	return errors.Trace(ctx.heap.err)
}

func (e *closureExecutor) newTopNSortRow() *sortRow {
	return &sortRow{
		key:  make([]types.Datum, len(e.evalContext.columnInfos)),
		data: make([][]byte, 2),
	}
}

func (e *topNProcessor) Finish() error {
	ctx := e.topNCtx
	sort.Sort(&ctx.heap.topNSorter)
	chk := e.scanCtx.chk
	for _, row := range ctx.heap.rows {
		err := e.processCore(row.data[0], row.data[1])
		if err != nil {
			return err
		}
		if chk.NumRows() == chunkMaxRows {
			if err = e.chunkToOldChunk(chk); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return e.chunkToOldChunk(chk)
}

type hashAggProcessor struct {
	skipVal
	*closureExecutor

	aggExprs     []aggregation.Aggregation
	groupByExprs []expression.Expression
	groups       map[string]struct{}
	groupKeys    [][]byte
	aggCtxsMap   map[string][]*aggregation.AggEvaluateContext
}

func (e *hashAggProcessor) Process(key, value []byte) (err error) {
	err = e.processCore(key, value)
	if err != nil {
		return err
	}
	if e.hasSelection() {
		gotRow, err1 := e.processSelection()
		if err1 != nil || !gotRow {
			return err1
		}
	}
	row := e.scanCtx.chk.GetRow(e.scanCtx.chk.NumRows() - 1)
	gk, err := e.getGroupKey(row)
	if _, ok := e.groups[string(gk)]; !ok {
		e.groups[string(gk)] = struct{}{}
		e.groupKeys = append(e.groupKeys, gk)
	}
	// Update aggregate expressions.
	aggCtxs := e.getContexts(gk)
	for i, agg := range e.aggExprs {
		err = agg.Update(aggCtxs[i], e.sc, row)
		if err != nil {
			return errors.Trace(err)
		}
	}
	e.scanCtx.chk.Reset()
	return nil
}

func (e *hashAggProcessor) getGroupKey(row chunk.Row) ([]byte, error) {
	length := len(e.groupByExprs)
	if length == 0 {
		return nil, nil
	}
	key := make([]byte, 0, 32)
	for _, item := range e.groupByExprs {
		v, err := item.Eval(row)
		if err != nil {
			return nil, errors.Trace(err)
		}
		b, err := codec.EncodeValue(e.sc, nil, v)
		if err != nil {
			return nil, errors.Trace(err)
		}
		key = append(key, b...)
	}
	return key, nil
}

func (e *hashAggProcessor) getContexts(groupKey []byte) []*aggregation.AggEvaluateContext {
	aggCtxs, ok := e.aggCtxsMap[string(groupKey)]
	if !ok {
		aggCtxs = make([]*aggregation.AggEvaluateContext, 0, len(e.aggExprs))
		for _, agg := range e.aggExprs {
			aggCtxs = append(aggCtxs, agg.CreateContext(e.sc))
		}
		e.aggCtxsMap[string(groupKey)] = aggCtxs
	}
	return aggCtxs
}

func (e *hashAggProcessor) Finish() error {
	for i, gk := range e.groupKeys {
		aggCtxs := e.getContexts(gk)
		e.oldRowBuf = e.oldRowBuf[:0]
		for i, agg := range e.aggExprs {
			partialResults := agg.GetPartialResult(aggCtxs[i])
			var err error
			e.oldRowBuf, err = codec.EncodeValue(e.sc, e.oldRowBuf, partialResults...)
			if err != nil {
				return err
			}
		}
		e.oldRowBuf = append(e.oldRowBuf, gk...)
		e.oldChunks = appendRow(e.oldChunks, e.oldRowBuf, i)
	}
	return nil
}

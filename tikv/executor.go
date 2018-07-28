package tikv

import (
	"bytes"
	"encoding/binary"
	"sort"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	"golang.org/x/net/context"
)

var (
	_ executor = &tableScanExec{}
	_ executor = &indexScanExec{}
	_ executor = &selectionExec{}
	_ executor = &limitExec{}
	_ executor = &topNExec{}
)

type executor interface {
	SetSrcExec(executor)
	GetSrcExec() executor
	ResetCounts()
	Counts() []int64
	Next(ctx context.Context) ([][]byte, error)
	nextChunk(ctx context.Context, chk *chunk.Chunk) error
	fieldTypes() []*types.FieldType
	// Cursor returns the key gonna to be scanned by the Next() function.
	Cursor() (key []byte, desc bool)
}

type tableScanExec struct {
	*tipb.TableScan
	colIDs         map[int64]int
	kvRanges       []kv.KeyRange
	startTS        uint64
	isolationLevel kvrpcpb.IsolationLevel
	mvccStore      *MVCCStore
	reqCtx         *requestCtx
	rangeCursor    int

	rowCursor   int
	rows        [][][]byte
	seekKey     []byte
	start       int
	counts      []int64
	ignoreLock  bool
	lockChecked bool

	// for chunk
	tps        []*types.FieldType
	cols       [][]byte
	loc        *time.Location
	handleOnly bool

	src executor
}

func (e *tableScanExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *tableScanExec) GetSrcExec() executor {
	return e.src
}

func (e *tableScanExec) ResetCounts() {
	if e.counts != nil {
		e.start = e.rangeCursor
		e.counts[e.start] = 0
	}
}

func (e *tableScanExec) Counts() []int64 {
	if e.counts == nil {
		return nil
	}
	if e.seekKey == nil {
		return e.counts[e.start:e.rangeCursor]
	}
	return e.counts[e.start : e.rangeCursor+1]
}

func (e *tableScanExec) Cursor() ([]byte, bool) {
	if len(e.seekKey) > 0 {
		return e.seekKey, e.Desc
	}

	if e.rangeCursor < len(e.kvRanges) {
		ran := e.kvRanges[e.rangeCursor]
		if ran.IsPoint() {
			return ran.StartKey, e.Desc
		}

		if e.Desc {
			return ran.EndKey, e.Desc
		}
		return ran.StartKey, e.Desc
	}

	if e.Desc {
		return e.kvRanges[len(e.kvRanges)-1].StartKey, e.Desc
	}
	return e.kvRanges[len(e.kvRanges)-1].EndKey, e.Desc
}

func (e *tableScanExec) refill() error {
	e.rowCursor = 0
	e.rows = e.rows[:0]
	return e.fillRows()
}

func (e *tableScanExec) getOneRow() [][]byte {
	if e.rowCursor < len(e.rows) {
		value := e.rows[e.rowCursor]
		e.rowCursor++
		return value
	}
	return nil
}

func (e *tableScanExec) Next(ctx context.Context) ([][]byte, error) {
	err := e.checkRangeLock()
	if err != nil {
		return nil, errors.Trace(err)
	}
	for {
		if value := e.getOneRow(); value != nil {
			return value, nil
		}
		if err = e.refill(); err != nil {
			return nil, errors.Trace(err)
		}
		if len(e.rows) == 0 {
			break
		}
	}
	return nil, nil
}

func (e *tableScanExec) checkRangeLock() error {
	if !e.ignoreLock && !e.lockChecked {
		for _, ran := range e.kvRanges {
			err := e.mvccStore.CheckRangeLock(e.startTS, ran.StartKey, ran.EndKey)
			if err != nil {
				return err
			}
		}
		e.lockChecked = true
	}
	return nil
}

const chunkMaxRows = 1024

func (e *tableScanExec) nextChunk(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	err := e.checkRangeLock()
	if err != nil {
		return errors.Trace(err)
	}
	for e.rangeCursor < len(e.kvRanges) {
		ran := e.kvRanges[e.rangeCursor]
		if ran.IsPoint() {
			err = e.fillChunkFromPoint(chk, ran.StartKey)
			if err != nil {
				return errors.Trace(err)
			}
			e.nextRange()
			if chk.NumRows() == chunkMaxRows {
				return nil
			}
		} else {
			err = e.fillChunkFromRange(chk, ran)
			if err != nil {
				return errors.Trace(err)
			}
			if chk.NumRows() == chunkMaxRows {
				// When chunk is full after fill from range, the range may not finished.
				// So we do not increase range cursor here.
				return nil
			}
			// If chunk is not full, the range is finished.
			e.nextRange()
		}
	}
	return nil
}

func (e *tableScanExec) nextRange() {
	e.rangeCursor++
	e.seekKey = nil
}

func (e *tableScanExec) fieldTypes() []*types.FieldType {
	return e.tps
}

func (e *tableScanExec) fillChunkFromPoint(chk *chunk.Chunk, key kv.Key) error {
	reader := e.reqCtx.getDBReader()
	val, err := reader.Get(key, e.startTS)
	if err != nil {
		return errors.Trace(err)
	}
	if len(val) == 0 {
		return nil
	}
	handle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		return errors.Errorf("invalid record key %q", key)
	}
	err = e.decodeChunkRow(handle, val, chk)
	return errors.Trace(err)
}

func (e *tableScanExec) decodeChunkRow(handle int64, rowData []byte, chk *chunk.Chunk) error {
	for i := range e.cols {
		e.cols[i] = nil
	}
	err := cutRowToBuf(rowData, e.colIDs, e.cols)
	if err != nil {
		return errors.Trace(err)
	}
	decoder := codec.NewDecoder(chk, e.loc)
	for i, val := range e.cols {
		col := e.Columns[i]
		if val == nil {
			if col.GetPkHandle() || col.ColumnId == model.ExtraHandleID {
				chk.AppendInt64(i, handle)
				continue
			}
			if col.DefaultVal != nil {
				val = col.DefaultVal
			} else {
				val = []byte{codec.NilFlag}
			}
		}
		_, err = decoder.DecodeOne(val, i, e.tps[i])
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *tableScanExec) fillChunkFromRange(chk *chunk.Chunk, ran kv.KeyRange) (err error) {
	if e.seekKey == nil {
		if e.Desc {
			e.seekKey = ran.EndKey
		} else {
			e.seekKey = ran.StartKey
		}
	}
	limit := chunkMaxRows - chk.NumRows()
	var lastKey []byte
	scanFunc := func(key, val []byte) error {
		lastKey = key
		handle, err := tablecodec.DecodeRowKey(key)
		if err != nil {
			return errors.Trace(err)
		}
		if e.handleOnly {
			chk.AppendInt64(0, handle)
			return nil
		}
		err = e.decodeChunkRow(handle, val, chk)
		// errors.Trace can't be inlined by compiler, let's check error explicitly
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	}
	reader := e.reqCtx.getDBReader()
	if e.Desc {
		err = reader.ReverseScan(ran.StartKey, e.seekKey, limit, e.startTS, scanFunc)
	} else {
		err = reader.Scan(e.seekKey, ran.EndKey, limit, e.startTS, scanFunc)
	}
	if err != nil {
		return errors.Trace(err)
	}
	if lastKey == nil {
		return
	}
	if e.Desc {
		e.seekKey = prefixPrev(lastKey)
	} else {
		e.seekKey = []byte(kv.Key(lastKey).PrefixNext())
	}
	return
}

func (e *tableScanExec) fillRows() error {
	for e.rangeCursor < len(e.kvRanges) {
		ran := e.kvRanges[e.rangeCursor]
		var err error
		if ran.IsPoint() {
			err = e.fillRowsFromPoint(ran)
			e.nextRange()
		} else {
			err = e.fillRowsFromRange(ran)
			if len(e.rows) == 0 {
				e.nextRange()
			}
		}
		if err != nil {
			return errors.Trace(err)
		}
		if len(e.rows) > 0 {
			return nil
		}
	}
	return nil
}

func (e *tableScanExec) fillRowsFromPoint(ran kv.KeyRange) error {
	reader := e.reqCtx.getDBReader()
	val, err := reader.Get(ran.StartKey, e.startTS)
	if err != nil {
		return errors.Trace(err)
	}
	if len(val) == 0 {
		return nil
	}
	handle, err := tablecodec.DecodeRowKey(ran.StartKey)
	if err != nil {
		return errors.Trace(err)
	}
	row, err := getRowData(e.Columns, e.colIDs, handle, val)
	if err != nil {
		return errors.Trace(err)
	}
	e.rows = append(e.rows, row)
	return nil
}

const scanLimit = 128

func (e *tableScanExec) fillRowsFromRange(ran kv.KeyRange) error {
	if e.seekKey == nil {
		if e.Desc {
			e.seekKey = ran.EndKey
		} else {
			e.seekKey = ran.StartKey
		}
	}
	var lastKey []byte
	scanFunc := func(key, value []byte) error {
		lastKey = key
		handle, err := tablecodec.DecodeRowKey(key)
		if err != nil {
			return errors.Trace(err)
		}
		row, err := getRowData(e.Columns, e.colIDs, handle, safeCopy(value))
		if err != nil {
			return errors.Trace(err)
		}
		e.rows = append(e.rows, row)
		return nil
	}
	reader := e.reqCtx.getDBReader()
	var err error
	if e.Desc {
		err = reader.ReverseScan(ran.StartKey, e.seekKey, scanLimit, e.startTS, scanFunc)
	} else {
		err = reader.Scan(e.seekKey, ran.EndKey, scanLimit, e.startTS, scanFunc)
	}
	if err != nil {
		return errors.Trace(err)
	}
	if lastKey == nil {
		return nil
	}
	if e.Desc {
		e.seekKey = prefixPrev(lastKey)
	} else {
		e.seekKey = []byte(kv.Key(lastKey).PrefixNext())
	}
	return nil
}

const (
	pkColNotExists = iota
	pkColIsSigned
	pkColIsUnsigned
)

type indexScanExec struct {
	*tipb.IndexScan
	colsLen        int
	kvRanges       []kv.KeyRange
	startTS        uint64
	isolationLevel kvrpcpb.IsolationLevel
	mvccStore      *MVCCStore
	reqCtx         *requestCtx
	ranCursor      int
	seekKey        []byte
	pkStatus       int
	start          int
	counts         []int64
	ignoreLock     bool
	lockChecked    bool

	rowCursor int
	rows      [][][]byte
	src       executor
	tps       []*types.FieldType
	loc       *time.Location
}

func (e *indexScanExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *indexScanExec) GetSrcExec() executor {
	return e.src
}

func (e *indexScanExec) ResetCounts() {
	if e.counts != nil {
		e.start = e.ranCursor
		e.counts[e.start] = 0
	}
}

func (e *indexScanExec) Counts() []int64 {
	if e.counts == nil {
		return nil
	}
	if e.seekKey == nil || e.ranCursor == len(e.counts) {
		return e.counts[e.start:e.ranCursor]
	}
	return e.counts[e.start : e.ranCursor+1]
}

func (e *indexScanExec) isUnique() bool {
	return e.Unique != nil && *e.Unique
}

func (e *indexScanExec) Cursor() ([]byte, bool) {
	if len(e.seekKey) > 0 {
		return e.seekKey, e.Desc
	}
	if e.ranCursor < len(e.kvRanges) {
		ran := e.kvRanges[e.ranCursor]
		if e.isUnique() && ran.IsPoint() {
			return ran.StartKey, e.Desc
		}
		if e.Desc {
			return ran.EndKey, e.Desc
		}
		return ran.StartKey, e.Desc
	}
	if e.Desc {
		return e.kvRanges[len(e.kvRanges)-1].StartKey, e.Desc
	}
	return e.kvRanges[len(e.kvRanges)-1].EndKey, e.Desc
}

func (e *indexScanExec) nextChunk(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	err := e.checkRangeLock()
	if err != nil {
		return errors.Trace(err)
	}
	for e.ranCursor < len(e.kvRanges) {
		ran := e.kvRanges[e.ranCursor]
		if e.isUnique() && ran.IsPoint() {
			err = e.fillChunkFromPoint(chk, ran.StartKey)
			if err != nil {
				return errors.Trace(err)
			}
			e.nextRange()
			if chk.NumRows() == chunkMaxRows {
				return nil
			}
		} else {
			err = e.fillChunkFromRange(chk, ran)
			if err != nil {
				return errors.Trace(err)
			}
			if chk.NumRows() == chunkMaxRows {
				// When chunk is full after fill from range, the range may not finished.
				// So we do not increase range cursor here.
				return nil
			}
			// If chunk is not full, the range is finished.
			e.nextRange()
		}
	}
	return nil
}

func (e *indexScanExec) fillChunkFromPoint(chk *chunk.Chunk, key kv.Key) error {
	reader := e.reqCtx.getDBReader()
	val, err := reader.Get(key, e.startTS)
	if err != nil {
		return errors.Trace(err)
	}
	if len(val) == 0 {
		return nil
	}
	err = e.decodeToChunk(chk, key, val)
	return errors.Trace(err)
}

func (e *indexScanExec) decodeToChunk(chk *chunk.Chunk, key kv.Key, val []byte) error {
	values, b, err := tablecodec.CutIndexKeyNew(key, e.colsLen)
	if err != nil {
		return errors.Trace(err)
	}
	decoder := codec.NewDecoder(chk, e.loc)
	for i, colVal := range values {
		_, err = decoder.DecodeOne(colVal, i, e.tps[i])
		if err != nil {
			return errors.Trace(err)
		}
	}
	if len(b) > 0 {
		if e.pkStatus != pkColNotExists {
			_, err = decoder.DecodeOne(b, e.colsLen, e.tps[e.colsLen])
			if err != nil {
				return errors.Trace(err)
			}
		}
	} else if e.pkStatus != pkColNotExists {
		handle, err := decodeHandle(val)
		if err != nil {
			return errors.Trace(err)
		}
		chk.AppendInt64(e.colsLen, handle)
	}
	return nil
}

func (e *indexScanExec) fillChunkFromRange(chk *chunk.Chunk, ran kv.KeyRange) (err error) {
	if e.seekKey == nil {
		if e.Desc {
			e.seekKey = ran.EndKey
		} else {
			e.seekKey = ran.StartKey
		}
	}
	limit := chunkMaxRows - chk.NumRows()
	var lastKey []byte
	scanFunc := func(key, value []byte) error {
		lastKey = key
		err = e.decodeToChunk(chk, key, value)
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	}
	reader := e.reqCtx.getDBReader()
	if e.Desc {
		err = reader.ReverseScan(ran.StartKey, e.seekKey, limit, e.startTS, scanFunc)
	} else {
		err = reader.Scan(e.seekKey, ran.EndKey, limit, e.startTS, scanFunc)
	}
	if err != nil {
		return errors.Trace(err)
	}
	if lastKey == nil {
		return
	}
	if e.Desc {
		e.seekKey = prefixPrev(lastKey)
	} else {
		e.seekKey = []byte(kv.Key(lastKey).PrefixNext())
	}
	return nil
}

func (e *indexScanExec) fieldTypes() []*types.FieldType {
	return e.tps
}

func (e *indexScanExec) checkRangeLock() error {
	if !e.ignoreLock && !e.lockChecked {
		for _, ran := range e.kvRanges {
			err := e.mvccStore.CheckRangeLock(e.startTS, ran.StartKey, ran.EndKey)
			if err != nil {
				return err
			}
		}
		e.lockChecked = true
	}
	return nil
}

func (e *indexScanExec) Next(ctx context.Context) (value [][]byte, err error) {
	err = e.checkRangeLock()
	if err != nil {
		return nil, errors.Trace(err)
	}
	for {
		if e.rowCursor < len(e.rows) {
			value = e.rows[e.rowCursor]
			e.rowCursor++
			return value, nil
		}
		e.rowCursor = 0
		e.rows = e.rows[:0]
		err = e.fillRows()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(e.rows) == 0 {
			break
		}
	}
	return nil, nil
}

func (e *indexScanExec) fillRows() error {
	for e.ranCursor < len(e.kvRanges) {
		ran := e.kvRanges[e.ranCursor]
		var err error
		if e.isUnique() && ran.IsPoint() {
			err = e.fillRowsFromPoint(ran)
			e.nextRange()
		} else {
			err = e.fillRowsFromRange(ran)
			if len(e.rows) == 0 {
				e.nextRange()
			}
		}
		if err != nil {
			return errors.Trace(err)
		}
		if len(e.rows) > 0 {
			break
		}
	}
	return nil
}

func (e *indexScanExec) nextRange() {
	e.ranCursor++
	e.seekKey = nil
}

// fillRowsFromPoint is only used for unique key.
func (e *indexScanExec) fillRowsFromPoint(ran kv.KeyRange) error {
	val, err := e.reqCtx.getDBReader().Get(ran.StartKey, e.startTS)
	if err != nil {
		return errors.Trace(err)
	}
	if len(val) == 0 {
		return nil
	}
	row, err := e.decodeIndexKV(ran.StartKey, val)
	if err != nil {
		return errors.Trace(err)
	}
	e.rows = append(e.rows, row)
	return nil
}

func (e *indexScanExec) decodeIndexKV(key, value []byte) ([][]byte, error) {
	values, b, err := tablecodec.CutIndexKeyNew(key, e.colsLen)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(b) > 0 {
		if e.pkStatus != pkColNotExists {
			values = append(values, b)
		}
	} else if e.pkStatus != pkColNotExists {
		handle, err := decodeHandle(value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		var handleDatum types.Datum
		if e.pkStatus == pkColIsUnsigned {
			handleDatum = types.NewUintDatum(uint64(handle))
		} else {
			handleDatum = types.NewIntDatum(handle)
		}
		handleBytes, err := codec.EncodeValue(nil, b, handleDatum)
		if err != nil {
			return nil, errors.Trace(err)
		}
		values = append(values, handleBytes)
	}
	return values, nil
}

func (e *indexScanExec) fillRowsFromRange(ran kv.KeyRange) error {
	if e.seekKey == nil {
		if e.Desc {
			e.seekKey = ran.EndKey
		} else {
			e.seekKey = ran.StartKey
		}
	}
	var lastKey []byte
	scanFunc := func(key, value []byte) error {
		lastKey = key
		row, err := e.decodeIndexKV(safeCopy(key), value)
		if err != nil {
			return errors.Trace(err)
		}
		e.rows = append(e.rows, row)
		return nil
	}
	var err error
	reader := e.reqCtx.getDBReader()
	if e.Desc {
		err = reader.ReverseScan(ran.StartKey, e.seekKey, scanLimit, e.startTS, scanFunc)
	} else {
		err = reader.Scan(e.seekKey, ran.EndKey, scanLimit, e.startTS, scanFunc)
	}
	if err != nil {
		return errors.Trace(err)
	}
	if lastKey == nil {
		return nil
	}
	if e.Desc {
		e.seekKey = prefixPrev(lastKey)
	} else {
		e.seekKey = []byte(kv.Key(lastKey).PrefixNext())
	}
	return nil
}

// previous version of kv.PrefixNext.
func prefixPrev(k []byte) []byte {
	buf := make([]byte, len([]byte(k)))
	copy(buf, []byte(k))
	var i int
	for i = len(k) - 1; i >= 0; i-- {
		buf[i]--
		if buf[i] != 255 {
			break
		}
	}
	if i == -1 {
		return nil
	}
	return buf
}

type selectionExec struct {
	conditions        []expression.Expression
	relatedColOffsets []int
	row               []types.Datum
	chkRow            chkMutRow
	evalCtx           *evalContext
	src               executor
	srcChk            *chunk.Chunk
	srcIter           *chunk.Iterator4Chunk
	selected          []bool
	seCtx             sessionctx.Context
	inputRow          chunk.Row
}

func (e *selectionExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *selectionExec) GetSrcExec() executor {
	return e.src
}

func (e *selectionExec) ResetCounts() {
	e.src.ResetCounts()
}

func (e *selectionExec) Counts() []int64 {
	return e.src.Counts()
}

// evalBool evaluates expression to a boolean value.
func (e *selectionExec) evalBool(exprs []expression.Expression, row []types.Datum, ctx *stmtctx.StatementContext) (bool, error) {
	e.chkRow.update(row)
	for _, expr := range exprs {
		data, err := expr.Eval(e.chkRow.row())
		if err != nil {
			return false, errors.Trace(err)
		}
		if data.IsNull() {
			return false, nil
		}

		isBool, err := data.ToBool(ctx)
		if err != nil {
			return false, errors.Trace(err)
		}
		if isBool == 0 {
			return false, nil
		}
	}
	return true, nil
}

func (e *selectionExec) Cursor() ([]byte, bool) {
	return e.src.Cursor()
}

func (e *selectionExec) Next(ctx context.Context) (value [][]byte, err error) {
	for {
		value, err = e.src.Next(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if value == nil {
			return nil, nil
		}

		err = e.evalCtx.decodeRelatedColumnVals(e.relatedColOffsets, value, e.row)
		if err != nil {
			return nil, errors.Trace(err)
		}
		match, err := e.evalBool(e.conditions, e.row, e.evalCtx.sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if match {
			return value, nil
		}
	}
}

func (e *selectionExec) nextChunk(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.srcChk == nil {
		e.srcChk = chunk.NewChunkWithCapacity(e.src.fieldTypes(), 16)
		e.srcIter = chunk.NewIterator4Chunk(e.srcChk)
	}
	for {
		for ; e.inputRow != e.srcIter.End(); e.inputRow = e.srcIter.Next() {
			if !e.selected[e.inputRow.Idx()] {
				continue
			}
			if chk.NumRows() == chunkMaxRows {
				return nil
			}
			chk.AppendRow(e.inputRow)
		}
		err := e.src.nextChunk(ctx, e.srcChk)
		if err != nil {
			return errors.Trace(err)
		}
		// no more data.
		if e.srcChk.NumRows() == 0 {
			return nil
		}
		e.selected, err = expression.VectorizedFilter(e.seCtx, e.conditions, e.srcIter, e.selected)
		if err != nil {
			return errors.Trace(err)
		}
		e.inputRow = e.srcIter.Begin()
	}
	return nil
}

func (e *selectionExec) fieldTypes() []*types.FieldType {
	return e.src.fieldTypes()
}

type topNExec struct {
	heap              *topNHeap
	evalCtx           *evalContext
	relatedColOffsets []int
	orderByExprs      []expression.Expression
	row               []types.Datum
	chkRow            chkMutRow
	cursor            int
	executed          bool

	src     executor
	srcChks *chunk.List
	rowPtrs []chunk.RowPtr
}

func (e *topNExec) SetSrcExec(src executor) {
	e.src = src
}

func (e *topNExec) GetSrcExec() executor {
	return e.src
}

func (e *topNExec) ResetCounts() {
	e.src.ResetCounts()
}

func (e *topNExec) Counts() []int64 {
	return e.src.Counts()
}

func (e *topNExec) innerNext(ctx context.Context) (bool, error) {
	value, err := e.src.Next(ctx)
	if err != nil {
		return false, errors.Trace(err)
	}
	if value == nil {
		return false, nil
	}
	err = e.evalTopN(value)
	if err != nil {
		return false, errors.Trace(err)
	}
	return true, nil
}

func (e *topNExec) Cursor() ([]byte, bool) {
	panic("don't not use coprocessor streaming API for topN!")
}

func (e *topNExec) Next(ctx context.Context) (value [][]byte, err error) {
	if !e.executed {
		for {
			hasMore, err := e.innerNext(ctx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !hasMore {
				break
			}
		}
		e.executed = true
	}
	if e.cursor >= len(e.heap.rows) {
		return nil, nil
	}
	sort.Sort(&e.heap.topNSorter)
	row := e.heap.rows[e.cursor]
	e.cursor++

	return row.data, nil
}

// evalTopN evaluates the top n elements from the data. The input receives a record including its handle and data.
// And this function will check if this record can replace one of the old records.
func (e *topNExec) evalTopN(value [][]byte) error {
	newRow := &sortRow{
		key: make([]types.Datum, len(value)),
	}
	err := e.evalCtx.decodeRelatedColumnVals(e.relatedColOffsets, value, e.row)
	if err != nil {
		return errors.Trace(err)
	}
	e.chkRow.update(e.row)
	for i, expr := range e.orderByExprs {
		newRow.key[i], err = expr.Eval(e.chkRow.row())
		if err != nil {
			return errors.Trace(err)
		}
	}

	if e.heap.tryToAddRow(newRow) {
		for _, val := range value {
			newRow.data = append(newRow.data, val)
		}
	}
	return errors.Trace(e.heap.err)
}

func (e *topNExec) nextChunk(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	return nil
}

func (e *topNExec) loadToLimit(ctx context.Context) error {
	for e.srcChks.Len() < e.heap.totalCount {
		srcChk := chunk.NewChunkWithCapacity(e.src.fieldTypes(), chunkMaxRows)
		err := e.src.nextChunk(ctx, srcChk)
		if err != nil {
			return errors.Trace(err)
		}
		if srcChk.NumRows() == 0 {
			break
		}
		e.srcChks.Add(srcChk)
	}
	e.initPointers()
	return nil
}

func (e *topNExec) initPointers() {
	e.rowPtrs = make([]chunk.RowPtr, 0, e.srcChks.Len())
	for chkIdx := 0; chkIdx < e.srcChks.NumChunks(); chkIdx++ {
		rowChk := e.srcChks.GetChunk(chkIdx)
		for rowIdx := 0; rowIdx < rowChk.NumRows(); rowIdx++ {
			e.rowPtrs = append(e.rowPtrs, chunk.RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)})
		}
	}
}

func (e *topNExec) initCompareFuncs() {
}

func (e *topNExec) fieldTypes() []*types.FieldType {
	return e.src.fieldTypes()
}

type limitExec struct {
	limit  uint64
	cursor uint64

	src executor
}

func (e *limitExec) SetSrcExec(src executor) {
	e.src = src
}

func (e *limitExec) GetSrcExec() executor {
	return e.src
}

func (e *limitExec) ResetCounts() {
	e.src.ResetCounts()
}

func (e *limitExec) Counts() []int64 {
	return e.src.Counts()
}

func (e *limitExec) Cursor() ([]byte, bool) {
	return e.src.Cursor()
}

func (e *limitExec) Next(ctx context.Context) (value [][]byte, err error) {
	if e.cursor >= e.limit {
		return nil, nil
	}

	value, err = e.src.Next(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if value == nil {
		return nil, nil
	}
	e.cursor++
	return value, nil
}

func (e *limitExec) nextChunk(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.cursor == e.limit {
		return nil
	}
	err := e.src.nextChunk(ctx, chk)
	if err != nil {
		return errors.Trace(err)
	}
	if chk.NumRows() == 0 {
		return nil
	}
	limitRemain := int(e.limit - e.cursor)
	if chk.NumRows() > limitRemain {
		chk.TruncateTo(limitRemain)
		e.cursor = e.limit
	} else {
		e.cursor += uint64(chk.NumRows())
	}
	return nil
}

func (e *limitExec) fieldTypes() []*types.FieldType {
	return e.src.fieldTypes()
}

func hasColVal(data [][]byte, colIDs map[int64]int, id int64) bool {
	offset, ok := colIDs[id]
	if ok && data[offset] != nil {
		return true
	}
	return false
}

// getRowData decodes raw byte slice to row data.
func getRowData(columns []*tipb.ColumnInfo, colIDs map[int64]int, handle int64, value []byte) ([][]byte, error) {
	values, err := tablecodec.CutRowNew(value, colIDs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if values == nil {
		values = make([][]byte, len(colIDs))
	}
	// Fill the handle and null columns.
	for _, col := range columns {
		id := col.GetColumnId()
		offset := colIDs[id]
		if col.GetPkHandle() || id == model.ExtraHandleID {
			var handleDatum types.Datum
			if mysql.HasUnsignedFlag(uint(col.GetFlag())) {
				// PK column is Unsigned.
				handleDatum = types.NewUintDatum(uint64(handle))
			} else {
				handleDatum = types.NewIntDatum(handle)
			}
			handleData, err1 := codec.EncodeValue(nil, nil, handleDatum)
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
			values[offset] = handleData
			continue
		}
		if hasColVal(values, colIDs, id) {
			continue
		}
		if len(col.DefaultVal) > 0 {
			values[offset] = col.DefaultVal
			continue
		}
		if mysql.HasNotNullFlag(uint(col.GetFlag())) {
			return nil, errors.Errorf("Miss column %d", id)
		}

		values[offset] = []byte{codec.NilFlag}
	}

	return values, nil
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

func decodeHandle(data []byte) (int64, error) {
	var h int64
	buf := bytes.NewBuffer(data)
	err := binary.Read(buf, binary.BigEndian, &h)
	return h, errors.Trace(err)
}

// cutRowToBuf cuts encoded row into byte slices.
// Row layout: colID1, value1, colID2, value2, .....
func cutRowToBuf(data []byte, colIDs map[int64]int, buf [][]byte) error {
	if data == nil {
		return nil
	}
	if len(data) == 1 && data[0] == codec.NilFlag {
		return nil
	}
	var (
		cnt int
		b   []byte
	)
	for len(data) > 0 && cnt < len(colIDs) {
		// Get col id.
		remain, cid, err := codec.DecodeVarint(data[1:])
		// Get col value.
		b, data, err = codec.CutOne(remain)
		if err != nil {
			return errors.Trace(err)
		}
		offset, ok := colIDs[int64(cid)]
		if ok {
			buf[offset] = b
			cnt++
		}
	}
	return nil
}

type chkMutRow struct {
	mutRow *chunk.MutRow
}

func (c *chkMutRow) update(row []types.Datum) {
	if c.mutRow == nil {
		chkRow := chunk.MutRowFromDatums(row)
		c.mutRow = &chkRow
	} else {
		c.mutRow.SetDatums(row...)
	}
}

func (c *chkMutRow) row() chunk.Row {
	return c.mutRow.ToRow()
}

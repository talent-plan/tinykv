package tikv

import (
	"fmt"
	"math"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/types"
	"github.com/juju/errors"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/pingcap/tidb/util/codec"
)

// tryBuildClosureExecutor tries to build a closureExecutor for the DAGRequest.
// currently, only 'count(*)' is supported, but we can support all kinds of requests in the future.
func (svr *Server) tryBuildClosureExecutor(dagCtx *dagContext, dagReq *tipb.DAGRequest) *closureExecutor {
	executors := dagReq.Executors
	if len(executors) != 2 {
		return nil
	}
	var cols []*tipb.ColumnInfo
	var unique bool
	switch executors[0].Tp {
	case tipb.ExecType_TypeTableScan:
		tblScan := executors[0].TblScan
		cols = tblScan.Columns
		unique = true
	case tipb.ExecType_TypeIndexScan:
		idxScan := executors[0].IdxScan
		cols = idxScan.Columns
		unique = idxScan.GetUnique()
	default:
		panic(fmt.Sprintf("unknown first executor type %s", executors[0].Tp))
	}
	if len(cols) != 1 {
		return nil
	}
	col := cols[0]
	if !col.PkHandle {
		return nil
	}
	agg := executors[1].Aggregation
	if agg == nil {
		return nil
	}
	if len(agg.AggFunc) != 1 {
		return nil
	}
	if len(agg.GroupBy) != 0 {
		return nil
	}
	aggFunc := agg.AggFunc[0]
	if aggFunc.Tp != tipb.ExprType_Count {
		return nil
	}
	if len(aggFunc.Children) != 1 {
		return nil
	}
	if aggFunc.Children[0].Tp != tipb.ExprType_Int64 {
		return nil
	}
	ranges, err := svr.extractKVRanges(dagCtx.reqCtx.regCtx, dagCtx.keyRanges, false)
	if err != nil {
		return nil
	}
	ce := &closureExecutor{
		dagCtx: dagCtx,
		mvccStore: svr.mvccStore,
		kvRanges: ranges,
		startTS: dagReq.StartTs,
		unique:  unique,
	}
	ce.processFunc = ce.countStarProcess
	ce.finishFunc = ce.countStarFinish
	return ce
}

// closureExecutor is an execution engine that flatten the DAGRequest.Executors to a single closure `processFunc` that
// process key/value pairs. We can define many closures for different kinds of requests, try to use the specially
// optimized one for some frequently used query.
type closureExecutor struct {
	dagCtx      *dagContext
	mvccStore   *MVCCStore
	kvRanges    []kv.KeyRange
	startTS     uint64
	ignoreLock  bool
	lockChecked bool
	rangeBuf    []byte

	count       int64
	unique      bool

	oldChunks   []tipb.Chunk
	processFunc func (key, val []byte) error
	finishFunc  func() error
}

func (e *closureExecutor) execute() ([]tipb.Chunk, error) {
	err := e.checkRangeLock()
	if err != nil {
		return nil, errors.Trace(err)
	}
	dbReader := e.dagCtx.reqCtx.getDBReader()
	for _, ran := range e.kvRanges {
		if ran.IsPoint() {
			val, err := dbReader.Get(ran.StartKey, e.startTS)
			if err != nil {
				return nil, errors.Trace(err)
			}
			err = e.processFunc(ran.StartKey, val)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			err = dbReader.Scan(ran.StartKey, ran.EndKey, math.MaxInt64, e.startTS, e.processFunc)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}
	err = e.finishFunc()
	return e.oldChunks, err
}

func (e *closureExecutor) checkRangeLock() error {
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

// countStarProcess is used for `count(*)`.
func (ce *closureExecutor) countStarProcess(key, value []byte) error {
	ce.count++
	return nil
}

// countStarFinish is used for `count(*)`.
func (ce *closureExecutor) countStarFinish() error {
	d := types.NewIntDatum(ce.count)
	rowData, err := codec.EncodeValue(ce.dagCtx.evalCtx.sc, nil, d)
	if err != nil {
		return errors.Trace(err)
	}
	ce.oldChunks = appendRow(ce.oldChunks, rowData, 0)
	return nil
}

package tikv

import (
	"fmt"
	"io/ioutil"
	_ "math"
	"os"
	"sync"
	"testing"

	"github.com/coocood/badger"
	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	_ "github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
	_ "golang.org/x/net/context"
)

const (
	keyNumber         = 3
	TableId           = 0
	StartTs           = 10
	TTL               = 60000
	DagRequestStartTs = 100
)

// wrapper of test data, including encoded data, column types etc.
type data struct {
	encodedTestKVDatas []*encodedTestKVData
	colInfos           []*tipb.ColumnInfo
	rows               map[int64][]types.Datum    // handle -> row
	colTypes           map[int64]*types.FieldType // colId -> fieldType
}

type encodedTestKVData struct {
	encodedRowKey   []byte
	encodedRowValue []byte
}

type testStore struct {
	mvccStore *MVCCStore
	svr       *Server
	dbPath    string
	vLogPath  string
}

func newTestStore() (*testStore, error) {
	dbPath, err := ioutil.TempDir("", "unistore_cop_db")
	if err != nil {
		return nil, err
	}
	vLogPath, err := ioutil.TempDir("", "unistore_cop_value")
	if err != nil {
		return nil, err
	}
	safePoint := &SafePoint{}
	db, err := createTestDB(dbPath, vLogPath)
	if err != nil {
		return nil, err
	}
	dbBundle := &mvcc.DBBundle{
		DB:            db,
		LockStore:     lockstore.NewMemStore(4096),
		RollbackStore: lockstore.NewMemStore(4096),
	}
	writer := NewDBWriter(dbBundle, safePoint)
	store := NewMVCCStore(dbBundle, dbPath, safePoint, writer)
	svr := &Server{mvccStore: store}
	return &testStore{
		mvccStore: store,
		svr:       svr,
		dbPath:    dbPath,
		vLogPath:  vLogPath,
	}, nil
}

func cleanTestStore(t *testing.T, store *testStore) {
	require.Nil(t, os.RemoveAll(store.dbPath))
	require.Nil(t, os.RemoveAll(store.vLogPath))
}

func (store *testStore) initTestData(encodedKVDatas []*encodedTestKVData) []error {
	reqCtx := requestCtx{
		regCtx: &regionCtx{
			latches: make(map[uint64]*sync.WaitGroup),
		},
		svr: store.svr,
	}
	i := 0
	for _, kvData := range encodedKVDatas {
		mutation := makeATestMutaion(kvrpcpb.Op_Put, kvData.encodedRowKey,
			kvData.encodedRowValue)
		req := &kvrpcpb.PrewriteRequest{
			Mutations:    []*kvrpcpb.Mutation{mutation},
			PrimaryLock:  kvData.encodedRowKey,
			StartVersion: uint64(StartTs + i),
			LockTtl:      TTL,
		}
		err := store.mvccStore.Prewrite(&reqCtx, req)
		if err != nil {
			return []error{err}
		}
		commitError := store.mvccStore.Commit(&reqCtx, [][]byte{kvData.encodedRowKey},
			uint64(StartTs+i), uint64(StartTs+i+1))
		if commitError != nil {
			return []error{commitError}
		}
		i += 2
	}
	return nil
}

func makeATestMutaion(op kvrpcpb.Op, key []byte, value []byte) *kvrpcpb.Mutation {
	return &kvrpcpb.Mutation{
		Op:    op,
		Key:   key,
		Value: value,
	}
}

func createTestDB(dbPath, vLogPath string) (*badger.DB, error) {
	subPath := fmt.Sprintf("/%d", 0)
	opts := badger.DefaultOptions
	opts.Dir = dbPath + subPath
	opts.ValueDir = vLogPath + subPath
	return badger.Open(opts)
}

func prepareTestTableData(t *testing.T, keyNumber int, tableId int64) *data {
	stmtCtx := new(stmtctx.StatementContext)
	colIds := []int64{1, 2, 3}
	colTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeDouble),
	}
	colInfos := make([]*tipb.ColumnInfo, 3)
	colTypeMap := map[int64]*types.FieldType{}
	for i := 0; i < 3; i++ {
		colInfos[i] = &tipb.ColumnInfo{
			ColumnId: colIds[i],
			Tp:       int32(colTypes[i].Tp),
		}
		colTypeMap[colIds[i]] = colTypes[i]
	}
	rows := map[int64][]types.Datum{}
	encodedTestKVDatas := make([]*encodedTestKVData, keyNumber)
	for i := 0; i < keyNumber; i++ {
		datum := types.MakeDatums(i, "abc", 10.0)
		rows[int64(i)] = datum
		rowEncodedData, err := tablecodec.EncodeRow(stmtCtx, datum, colIds, nil, nil)
		require.Nil(t, err)
		rowKeyEncodedData := tablecodec.EncodeRowKeyWithHandle(tableId, int64(i))
		encodedTestKVDatas[i] = &encodedTestKVData{encodedRowKey: rowKeyEncodedData, encodedRowValue: rowEncodedData}
	}
	return &data{
		colInfos:           colInfos,
		encodedTestKVDatas: encodedTestKVDatas,
		rows:               rows,
		colTypes:           colTypeMap,
	}
}

func getTestPointRange(tableId int64, handle int64) kv.KeyRange {
	startKey := tablecodec.EncodeRowKeyWithHandle(tableId, handle)
	endKey := make([]byte, len(startKey))
	copy(endKey, startKey)
	convertToPrefixNext(endKey)
	return kv.KeyRange{
		StartKey: startKey,
		EndKey:   endKey,
	}
}

// convert this key to the smallest key which is larger than the key given.
// see tikv/src/coprocessor/util.rs for more detail.
func convertToPrefixNext(key []byte) []byte {
	if key == nil || len(key) == 0 {
		return []byte{0}
	}
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == 255 {
			key[i] = 0
		} else {
			key[i] += 1
			return key
		}
	}
	for i := 0; i < len(key); i++ {
		key[i] = 255
	}
	return append(key, 0)
}

// return whether these two keys are equal.
func isPrefixNext(key []byte, expected []byte) bool {
	key = convertToPrefixNext(key)
	if len(key) != len(expected) {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] != expected[i] {
			return false
		}
	}
	return true
}

// return a dag context according to dagReq and key ranges.
func newDagContext(store *testStore, keyRanges []kv.KeyRange, dagReq *tipb.DAGRequest) *dagContext {
	sc := flagsToStatementContext(dagReq.Flags)
	dagCtx := &dagContext{
		reqCtx: &requestCtx{
			svr: store.svr,
			regCtx: &regionCtx{
				meta: &metapb.Region{
					StartKey: nil,
					EndKey:   nil,
				},
			},
		},
		dagReq:  dagReq,
		evalCtx: &evalContext{sc: sc},
	}
	if dagReq.Executors[0].Tp == tipb.ExecType_TypeTableScan {
		dagCtx.evalCtx.setColumnInfo(dagReq.Executors[0].TblScan.Columns)
	} else {
		dagCtx.evalCtx.setColumnInfo(dagReq.Executors[0].IdxScan.Columns)
	}
	dagCtx.keyRanges = make([]*coprocessor.KeyRange, len(keyRanges))
	for i, keyRange := range keyRanges {
		dagCtx.keyRanges[i] = &coprocessor.KeyRange{
			Start: keyRange.StartKey,
			End:   keyRange.EndKey,
		}
	}
	return dagCtx
}

// build and execute the executors according to the dagRequest and dagContext,
// return the result chunk data, rows count and err if occurs.
/*
func (store *testStore) buildExecutorsAndExecute(dagRequest *tipb.DAGRequest,
	dagCtx *dagContext) ([]tipb.Chunk, int, error) {
	closureExec, err := store.svr.buildClosureExecutor(dagCtx, dagRequest)
	if err != nil {
		return nil, 0, err
	}
	if closureExec != nil {
		chunks, err := closureExec.execute()
		if err != nil {
			return nil, 0, err
		}
		return chunks, closureExec.rowCount, nil
	}
	e, err := store.svr.buildDAGExecutor(dagCtx, dagRequest.Executors)
	if err != nil {
		return nil, 0, err
	}
	rowCnt := 0
	ctx := context.TODO()
	var chunks []tipb.Chunk
	for {
		var row [][]byte
		e.Counts()
		row, err = e.Next(ctx)
		if err != nil {
			break
		}
		if row == nil {
			break
		}
		data := dummySlice
		for _, offset := range dagRequest.OutputOffsets {
			data = append(data, row[offset]...)
		}
		chunks = appendRow(chunks, data, rowCnt)
		rowCnt++
	}
	return chunks, rowCnt, nil
}
*/

// dagBuilder is used to build dag request
type dagBuilder struct {
	startTs       uint64
	executors     []*tipb.Executor
	outputOffsets []uint32
}

// return a default dagBuilder
func newDagBuilder() *dagBuilder {
	return &dagBuilder{executors: make([]*tipb.Executor, 0)}
}

func (dagBuilder *dagBuilder) setStartTs(startTs uint64) *dagBuilder {
	dagBuilder.startTs = startTs
	return dagBuilder
}

func (dagBuilder *dagBuilder) setOutputOffsets(outputOffsets []uint32) *dagBuilder {
	dagBuilder.outputOffsets = outputOffsets
	return dagBuilder
}

func (dagBuilder *dagBuilder) addTableScan(colInfos []*tipb.ColumnInfo, tableId int64) *dagBuilder {
	dagBuilder.executors = append(dagBuilder.executors, &tipb.Executor{
		Tp: tipb.ExecType_TypeTableScan,
		TblScan: &tipb.TableScan{
			Columns: colInfos,
			TableId: tableId,
		},
	})
	return dagBuilder
}

func (dagBuilder *dagBuilder) build() *tipb.DAGRequest {
	return &tipb.DAGRequest{
		StartTs:       dagBuilder.startTs,
		Executors:     dagBuilder.executors,
		OutputOffsets: dagBuilder.outputOffsets,
	}
}

// see tikv/src/coprocessor/util.rs for more detail
func TestIsPrefixNext(t *testing.T) {
	require.True(t, isPrefixNext([]byte{}, []byte{0}))
	require.True(t, isPrefixNext([]byte{0}, []byte{1}))
	require.True(t, isPrefixNext([]byte{1}, []byte{2}))
	require.True(t, isPrefixNext([]byte{255}, []byte{255, 0}))
	require.True(t, isPrefixNext([]byte{255, 255, 255}, []byte{255, 255, 255, 0}))
	require.True(t, isPrefixNext([]byte{1, 255}, []byte{2, 0}))
	require.True(t, isPrefixNext([]byte{0, 1, 255}, []byte{0, 2, 0}))
	require.True(t, isPrefixNext([]byte{0, 1, 255, 5}, []byte{0, 1, 255, 6}))
	require.True(t, isPrefixNext([]byte{0, 1, 5, 255}, []byte{0, 1, 6, 0}))
	require.True(t, isPrefixNext([]byte{0, 1, 255, 255}, []byte{0, 2, 0, 0}))
	require.True(t, isPrefixNext([]byte{0, 255, 255, 255}, []byte{1, 0, 0, 0}))
}

func TestPointGet(t *testing.T) {
	// here would build mvccStore and server, and prepare
	// three rows data, just like the test data of table_scan.rs.
	// then init the store with the generated data.
	data := prepareTestTableData(t, keyNumber, TableId)
	store, err := newTestStore()
	defer cleanTestStore(t, store)
	require.Nil(t, err)
	errors := store.initTestData(data.encodedTestKVDatas)
	require.Nil(t, errors)
	/*
		handle := int64(math.MinInt64)
		// point get should return nothing when handle is math.MinInt64
		dagRequest := newDagBuilder().
			setStartTs(DagRequestStartTs).
			addTableScan(data.colInfos, TableId).
			setOutputOffsets([]uint32{0, 1}).
			build()
		dagCtx := newDagContext(store, []kv.KeyRange{getTestPointRange(TableId, handle)},
			dagRequest)
		chunks, rowCount, err := store.buildExecutorsAndExecute(dagRequest, dagCtx)
		require.Nil(t, err)
		require.Equal(t, rowCount, 0)
		// point get should return one row when handle = 0
		handle = 0
		dagRequest = newDagBuilder().
			setStartTs(DagRequestStartTs).
			addTableScan(data.colInfos, TableId).
			setOutputOffsets([]uint32{0, 1}).
			build()
		dagCtx = newDagContext(store, []kv.KeyRange{getTestPointRange(TableId, handle)},
			dagRequest)
		chunks, rowCount, err = store.buildExecutorsAndExecute(dagRequest, dagCtx)
		require.Nil(t, err)
		require.Equal(t, 1, rowCount)
		returnedRow, err := codec.Decode(chunks[0].RowsData, 2)
		require.Nil(t, err)
		// returned row should has 2 cols
		require.Equal(t, len(returnedRow), 2)

		expectedRow := data.rows[handle]
		eq, err := returnedRow[0].CompareDatum(nil, &expectedRow[0])
		require.Nil(t, err)
		require.Equal(t, eq, 0)
		eq, err = returnedRow[1].CompareDatum(nil, &expectedRow[1])
		require.Nil(t, err)
		require.Equal(t, eq, 0)
	*/
}

package tikv

import (
	"math"
	"time"

	"github.com/coocood/badger/y"
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/unistore/rowcodec"
	"github.com/ngaut/unistore/tikv/dbreader"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	"golang.org/x/net/context"
)

func (svr *Server) handleCopAnalyzeRequest(reqCtx *requestCtx, req *coprocessor.Request) *coprocessor.Response {
	resp := &coprocessor.Response{}
	if len(req.Ranges) == 0 {
		return resp
	}
	if req.GetTp() != kv.ReqTypeAnalyze {
		return resp
	}
	analyzeReq := new(tipb.AnalyzeReq)
	err := proto.Unmarshal(req.Data, analyzeReq)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}
	ranges, err := svr.extractKVRanges(reqCtx.regCtx, req.Ranges, false)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}
	y.Assert(len(ranges) == 1)
	if analyzeReq.Tp == tipb.AnalyzeType_TypeIndex {
		resp, err = svr.handleAnalyzeIndexReq(reqCtx, ranges[0], analyzeReq)
	} else {
		resp, err = svr.handleAnalyzeColumnsReq(reqCtx, ranges[0], analyzeReq)
	}
	if err != nil {
		resp = &coprocessor.Response{
			OtherError: err.Error(),
		}
	}
	return resp
}

func (svr *Server) handleAnalyzeIndexReq(reqCtx *requestCtx, ran kv.KeyRange, analyzeReq *tipb.AnalyzeReq) (*coprocessor.Response, error) {
	dbReader := reqCtx.getDBReader()
	processor := &analyzeIndexProcessor{
		colLen:       int(analyzeReq.IdxReq.NumColumns),
		statsBuilder: statistics.NewSortedBuilder(flagsToStatementContext(analyzeReq.Flags), analyzeReq.IdxReq.BucketSize, 0, types.NewFieldType(mysql.TypeBlob)),
	}
	if analyzeReq.IdxReq.CmsketchDepth != nil && analyzeReq.IdxReq.CmsketchWidth != nil {
		processor.cms = statistics.NewCMSketch(*analyzeReq.IdxReq.CmsketchDepth, *analyzeReq.IdxReq.CmsketchWidth)
	}
	err := dbReader.Scan(ran.StartKey, ran.EndKey, math.MaxInt64, analyzeReq.StartTs, processor)
	if err != nil {
		return nil, err
	}
	hg := statistics.HistogramToProto(processor.statsBuilder.Hist())
	var cm *tipb.CMSketch
	if processor.cms != nil {
		cm = statistics.CMSketchToProto(processor.cms)
	}
	data, err := proto.Marshal(&tipb.AnalyzeIndexResp{Hist: hg, Cms: cm})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &coprocessor.Response{Data: data}, nil
}

type analyzeIndexProcessor struct {
	skipVal

	colLen       int
	statsBuilder *statistics.SortedBuilder
	cms          *statistics.CMSketch
	rowBuf       []byte
}

func (p *analyzeIndexProcessor) Process(key, value []byte) error {
	values, _, err := tablecodec.CutIndexKeyNew(key, p.colLen)
	if err != nil {
		return err
	}
	p.rowBuf = p.rowBuf[:0]
	for _, val := range values {
		p.rowBuf = append(p.rowBuf, val...)
	}
	rowData := safeCopy(p.rowBuf)
	err = p.statsBuilder.Iterate(types.NewBytesDatum(rowData))
	if err != nil {
		return err
	}
	if p.cms != nil {
		p.cms.InsertBytes(rowData)
	}
	return nil
}

type analyzeColumnsExec struct {
	skipVal
	reader  *dbreader.DBReader
	seekKey []byte
	endKey  []byte
	startTS uint64

	chk     *chunk.Chunk
	decoder *rowcodec.Decoder
	req     *chunk.Chunk
	evalCtx *evalContext
	fields  []*ast.ResultField
}

func (svr *Server) handleAnalyzeColumnsReq(reqCtx *requestCtx, ran kv.KeyRange, analyzeReq *tipb.AnalyzeReq) (*coprocessor.Response, error) {
	sc := flagsToStatementContext(analyzeReq.Flags)
	sc.TimeZone = time.FixedZone("UTC", int(analyzeReq.TimeZoneOffset))
	evalCtx := &evalContext{sc: sc}
	columns := analyzeReq.ColReq.ColumnsInfo
	evalCtx.setColumnInfo(columns)
	decoder, err := evalCtx.newRowDecoder()
	if err != nil {
		return nil, err
	}
	e := &analyzeColumnsExec{
		reader:  reqCtx.getDBReader(),
		seekKey: ran.StartKey,
		endKey:  ran.EndKey,
		startTS: analyzeReq.StartTs,
		chk:     chunk.NewChunkWithCapacity(evalCtx.fieldTps, 1),
		decoder: decoder,
		evalCtx: evalCtx,
	}
	e.fields = make([]*ast.ResultField, len(columns))
	for i := range e.fields {
		rf := new(ast.ResultField)
		rf.Column = new(model.ColumnInfo)
		rf.Column.FieldType = types.FieldType{Tp: mysql.TypeBlob, Flen: mysql.MaxBlobWidth, Charset: charset.CharsetUTF8, Collate: charset.CollationUTF8}
		e.fields[i] = rf
	}

	pkID := int64(-1)
	numCols := len(columns)
	if columns[0].GetPkHandle() {
		pkID = columns[0].ColumnId
		numCols--
	}
	colReq := analyzeReq.ColReq
	builder := statistics.SampleBuilder{
		Sc:              sc,
		RecordSet:       e,
		ColLen:          numCols,
		MaxBucketSize:   colReq.BucketSize,
		MaxFMSketchSize: colReq.SketchSize,
		MaxSampleSize:   colReq.SampleSize,
	}
	if pkID != -1 {
		builder.PkBuilder = statistics.NewSortedBuilder(sc, builder.MaxBucketSize, pkID, types.NewFieldType(mysql.TypeBlob))
	}
	if colReq.CmsketchWidth != nil && colReq.CmsketchDepth != nil {
		builder.CMSketchWidth = *colReq.CmsketchWidth
		builder.CMSketchDepth = *colReq.CmsketchDepth
	}
	collectors, pkBuilder, err := builder.CollectColumnStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	colResp := &tipb.AnalyzeColumnsResp{}
	if pkID != -1 {
		colResp.PkHist = statistics.HistogramToProto(pkBuilder.Hist())
	}
	for _, c := range collectors {
		colResp.Collectors = append(colResp.Collectors, statistics.SampleCollectorToProto(c))
	}
	data, err := proto.Marshal(colResp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &coprocessor.Response{Data: data}, nil
}

// Fields implements the sqlexec.RecordSet Fields interface.
func (e *analyzeColumnsExec) Fields() []*ast.ResultField {
	return e.fields
}

func (e *analyzeColumnsExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	e.req = req
	err := e.reader.Scan(e.seekKey, e.endKey, math.MaxInt64, e.startTS, e)
	if err != nil {
		return err
	}
	if req.NumRows() < req.Capacity() {
		e.seekKey = e.endKey
	}
	return nil
}

func (e *analyzeColumnsExec) Process(key, value []byte) error {
	handle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		return errors.Trace(err)
	}
	err = e.decoder.Decode(value, handle, e.chk)
	if err != nil {
		return errors.Trace(err)
	}
	row := e.chk.GetRow(0)
	for i, tp := range e.evalCtx.fieldTps {
		d := row.GetDatum(i, tp)
		value, err := codec.EncodeValue(e.evalCtx.sc, nil, d)
		if err != nil {
			return err
		}
		e.req.AppendBytes(i, value)
	}
	e.chk.Reset()
	if e.req.NumRows() == e.req.Capacity() {
		e.seekKey = kv.Key(key).PrefixNext()
		return dbreader.ScanBreak
	}
	return nil
}

func (e *analyzeColumnsExec) NewChunk() *chunk.Chunk {
	fields := make([]*types.FieldType, 0, len(e.fields))
	for _, field := range e.fields {
		fields = append(fields, &field.Column.FieldType)
	}
	return chunk.NewChunkWithCapacity(fields, 1024)
}

// Close implements the sqlexec.RecordSet Close interface.
func (e *analyzeColumnsExec) Close() error {
	return nil
}

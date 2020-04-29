// Copyright 2019-present PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright 2019-present PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package coprocessor

import (
	"bytes"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor/rowcodec"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	"golang.org/x/net/context"
)

func (svr *CopHandler) HandleCopAnalyzeRequest(reader storage.StorageReader, req *coprocessor.Request) *coprocessor.Response {
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
	ranges, err := svr.extractKVRanges(reader, req.Ranges, false)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}
	y.Assert(len(ranges) == 1)
	if analyzeReq.Tp == tipb.AnalyzeType_TypeIndex {
		resp, err = svr.handleAnalyzeIndexReq(reader, ranges[0], analyzeReq, req.StartTs)
	} else {
		resp, err = svr.handleAnalyzeColumnsReq(reader, ranges[0], analyzeReq, req.StartTs)
	}
	if err != nil {
		resp = &coprocessor.Response{
			OtherError: err.Error(),
		}
	}
	return resp
}

func (svr *CopHandler) handleAnalyzeIndexReq(reader storage.StorageReader, ran kv.KeyRange, analyzeReq *tipb.AnalyzeReq, startTS uint64) (*coprocessor.Response, error) {
	processor := &analyzeIndexProcessor{
		colLen:       int(analyzeReq.IdxReq.NumColumns),
		statsBuilder: statistics.NewSortedBuilder(flagsToStatementContext(analyzeReq.Flags), analyzeReq.IdxReq.BucketSize, 0, types.NewFieldType(mysql.TypeBlob)),
	}
	if analyzeReq.IdxReq.CmsketchDepth != nil && analyzeReq.IdxReq.CmsketchWidth != nil {
		processor.cms = statistics.NewCMSketch(*analyzeReq.IdxReq.CmsketchDepth, *analyzeReq.IdxReq.CmsketchWidth)
	}

	txn := mvcc.RoTxn{Reader: reader, StartTS: startTS}
	scanner := mvcc.NewScanner(ran.StartKey, &txn)
	defer scanner.Close()
	for {
		key, val, err := scanner.Next()
		if err != nil {
			return nil, err
		}
		if key == nil && val == nil {
			break
		}
		if bytes.Compare(key, ran.EndKey) >= 0 {
			break
		}

		err = processor.Process(key, val)
		if err != nil {
			if err == ScanBreak {
				break
			}
			return nil, err
		}
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
	rowData := append([]byte{}, p.rowBuf...)
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
	reader  storage.StorageReader
	seekKey []byte
	endKey  []byte
	startTS uint64

	chk     *chunk.Chunk
	decoder *rowcodec.Decoder
	req     *chunk.Chunk
	evalCtx *evalContext
	fields  []*ast.ResultField
}

func (svr *CopHandler) handleAnalyzeColumnsReq(reader storage.StorageReader, ran kv.KeyRange, analyzeReq *tipb.AnalyzeReq, startTS uint64) (*coprocessor.Response, error) {
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
		reader:  reader,
		seekKey: ran.StartKey,
		endKey:  ran.EndKey,
		startTS: startTS,
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
	processor := e
	txn := mvcc.RoTxn{Reader: e.reader, StartTS: e.startTS}
	scanner := mvcc.NewScanner(e.seekKey, &txn)
	defer scanner.Close()
	for {
		key, val, err := scanner.Next()
		if err != nil {
			return err
		}
		if key == nil && val == nil {
			break
		}
		if bytes.Compare(key, e.endKey) >= 0 {
			break
		}

		err = processor.Process(key, val)
		if err != nil {
			if err == ScanBreak {
				break
			}
			return err
		}
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
		return ScanBreak
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

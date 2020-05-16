// Copyright 2016 PingCAP, Inc.
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

package tablecodec

import (
	"bytes"
	"encoding/binary"
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/structure"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/rowcodec"
)

var (
	tablePrefix     = []byte{'t'}
	recordPrefixSep = []byte("_r")
	indexPrefixSep  = []byte("_i")
)

const (
	idLen     = 8
	prefixLen = 1 + idLen /*tableID*/ + 2
	// RecordRowKeyLen is public for calculating avgerage row size.
	RecordRowKeyLen       = prefixLen + idLen /*handle*/
	tablePrefixLength     = 1
	recordPrefixSepLength = 2
)

// TableSplitKeyLen is the length of key 't{table_id}' which is used for table split.
const TableSplitKeyLen = 1 + idLen

// TablePrefix returns table's prefix 't'.
func TablePrefix() []byte {
	return tablePrefix
}

// appendTableRecordPrefix appends table record prefix  "t[tableID]_r".
func appendTableRecordPrefix(buf []byte, tableID int64) []byte {
	buf = append(buf, tablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	buf = append(buf, recordPrefixSep...)
	return buf
}

// EncodeRowKeyWithHandle encodes the table id, row handle into a kv.Key
func EncodeRowKeyWithHandle(tableID int64, handle int64) kv.Key {
	buf := make([]byte, 0, RecordRowKeyLen)
	buf = appendTableRecordPrefix(buf, tableID)
	buf = codec.EncodeInt(buf, handle)
	return buf
}

// DecodeRecordKey decodes the key and gets the tableID, handle.
func DecodeRecordKey(key kv.Key) (tableID int64, handle int64, err error) {
	/* Your code here */
	return
}

// appendTableIndexPrefix appends table index prefix  "t[tableID]_i".
func appendTableIndexPrefix(buf []byte, tableID int64) []byte {
	buf = append(buf, tablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	buf = append(buf, indexPrefixSep...)
	return buf
}

// EncodeIndexSeekKey encodes an index value to kv.Key.
func EncodeIndexSeekKey(tableID int64, idxID int64, encodedValue []byte) kv.Key {
	key := make([]byte, 0, prefixLen+len(encodedValue))
	key = appendTableIndexPrefix(key, tableID)
	key = codec.EncodeInt(key, idxID)
	key = append(key, encodedValue...)
	return key
}

// DecodeIndexKeyPrefix decodes the key and gets the tableID, indexID, indexValues.
func DecodeIndexKeyPrefix(key kv.Key) (tableID int64, indexID int64, indexValues []byte, err error) {
	/* Your code here */
	return tableID, indexID, indexValues, nil
}

// DecodeIndexKey decodes the key and gets the tableID, indexID, indexValues.
func DecodeIndexKey(key kv.Key) (tableID int64, indexID int64, indexValues []string, err error) {
	k := key

	tableID, indexID, key, err = DecodeIndexKeyPrefix(key)
	if err != nil {
		return 0, 0, nil, errors.Trace(err)
	}

	for len(key) > 0 {
		remain, d, e := codec.DecodeOne(key)
		if e != nil {
			return 0, 0, nil, errInvalidIndexKey.GenWithStack("invalid index key - %q %v", k, e)
		}
		str, e1 := d.ToString()
		if e1 != nil {
			return 0, 0, nil, errInvalidIndexKey.GenWithStack("invalid index key - %q %v", k, e1)
		}
		indexValues = append(indexValues, str)
		key = remain
	}
	return
}

// EncodeRow encode row data and column ids into a slice of byte.
// Row layout: colID1, value1, colID2, value2, .....
// valBuf and values pass by caller, for reducing EncodeRow allocates temporary bufs. If you pass valBuf and values as nil,
// EncodeRow will allocate it.
func EncodeRow(sc *stmtctx.StatementContext, row []types.Datum, colIDs []int64, valBuf []byte, values []types.Datum, rd *rowcodec.Encoder) ([]byte, error) {
	if len(row) != len(colIDs) {
		return nil, errors.Errorf("EncodeRow error: data and columnID count not match %d vs %d", len(row), len(colIDs))
	}
	return rd.Encode(sc, colIDs, row, valBuf)
}

// EncodeRowKey encodes the table id and record handle into a kv.Key
func EncodeRowKey(tableID int64, encodedHandle []byte) kv.Key {
	buf := make([]byte, 0, RecordRowKeyLen)
	buf = appendTableRecordPrefix(buf, tableID)
	buf = append(buf, encodedHandle...)
	return buf
}

// CutRowKeyPrefix cuts the row key prefix.
func CutRowKeyPrefix(key kv.Key) []byte {
	return key[prefixLen:]
}

// EncodeRecordKey encodes the recordPrefix, row handle into a kv.Key.
func EncodeRecordKey(recordPrefix kv.Key, h int64) kv.Key {
	buf := make([]byte, 0, len(recordPrefix)+idLen)
	buf = append(buf, recordPrefix...)
	buf = codec.EncodeInt(buf, h)
	return buf
}

func hasTablePrefix(key kv.Key) bool {
	return key[0] == tablePrefix[0]
}

func hasRecordPrefixSep(key kv.Key) bool {
	return key[0] == recordPrefixSep[0] && key[1] == recordPrefixSep[1]
}

// DecodeMetaKey decodes the key and get the meta key and meta field.
func DecodeMetaKey(ek kv.Key) (key []byte, field []byte, err error) {
	var tp uint64
	prefix := []byte("m")
	if !bytes.HasPrefix(ek, prefix) {
		return nil, nil, errors.New("invalid encoded hash data key prefix")
	}
	ek = ek[len(prefix):]
	ek, key, err = codec.DecodeBytes(ek, nil)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	ek, tp, err = codec.DecodeUint(ek)
	if err != nil {
		return nil, nil, errors.Trace(err)
	} else if structure.TypeFlag(tp) != structure.HashData {
		return nil, nil, errors.Errorf("invalid encoded hash data key flag %c", byte(tp))
	}
	_, field, err = codec.DecodeBytes(ek, nil)
	return key, field, errors.Trace(err)
}

// DecodeKeyHead decodes the key's head and gets the tableID, indexID. isRecordKey is true when is a record key.
func DecodeKeyHead(key kv.Key) (tableID int64, indexID int64, isRecordKey bool, err error) {
	isRecordKey = false
	k := key
	if !key.HasPrefix(tablePrefix) {
		err = errInvalidKey.GenWithStack("invalid key - %q", k)
		return
	}

	key = key[len(tablePrefix):]
	key, tableID, err = codec.DecodeInt(key)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	if key.HasPrefix(recordPrefixSep) {
		isRecordKey = true
		return
	}
	if !key.HasPrefix(indexPrefixSep) {
		err = errInvalidKey.GenWithStack("invalid key - %q", k)
		return
	}

	key = key[len(indexPrefixSep):]

	key, indexID, err = codec.DecodeInt(key)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	return
}

// DecodeTableID decodes the table ID of the key, if the key is not table key, returns 0.
func DecodeTableID(key kv.Key) int64 {
	if !key.HasPrefix(tablePrefix) {
		return 0
	}
	key = key[len(tablePrefix):]
	_, tableID, err := codec.DecodeInt(key)
	// TODO: return error.
	terror.Log(errors.Trace(err))
	return tableID
}

// DecodeRowKey decodes the key and gets the handle.
func DecodeRowKey(key kv.Key) (int64, error) {
	if len(key) != RecordRowKeyLen || !hasTablePrefix(key) || !hasRecordPrefixSep(key[prefixLen-2:]) {
		return 0, errInvalidKey.GenWithStack("invalid key - %q", key)
	}
	u := binary.BigEndian.Uint64(key[prefixLen:])
	return codec.DecodeCmpUintToInt(u), nil
}

// EncodeValue encodes a go value to bytes.
func EncodeValue(sc *stmtctx.StatementContext, b []byte, raw types.Datum) ([]byte, error) {
	return codec.EncodeValue(sc, b, raw)
}

// DecodeColumnValue decodes data to a Datum according to the column info.
func DecodeColumnValue(data []byte, ft *types.FieldType, loc *time.Location) (types.Datum, error) {
	_, d, err := codec.DecodeOne(data)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return d, nil
}

// DecodeRowWithMap decodes a byte slice into datums with a existing row map.
// Row layout: colID1, value1, colID2, value2, .....
func DecodeRowWithMap(b []byte, cols map[int64]*types.FieldType, loc *time.Location, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	if row == nil {
		row = make(map[int64]types.Datum, len(cols))
	}
	if b == nil {
		return row, nil
	}
	if len(b) == 1 && b[0] == codec.NilFlag {
		return row, nil
	}
	cnt := 0
	var (
		data []byte
		err  error
	)
	for len(b) > 0 {
		// Get col id.
		data, b, err = codec.CutOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		_, cid, err := codec.DecodeOne(data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Get col value.
		data, b, err = codec.CutOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		id := cid.GetInt64()
		ft, ok := cols[id]
		if ok {
			_, v, err := codec.DecodeOne(data)
			if err != nil {
				return nil, errors.Trace(err)
			}
			v, err = unflatten(v, ft, loc)
			if err != nil {
				return nil, errors.Trace(err)
			}
			row[id] = v
			cnt++
			if cnt == len(cols) {
				// Get enough data.
				break
			}
		}
	}
	return row, nil
}

// DecodeRow decodes a byte slice into datums.
// Row layout: colID1, value1, colID2, value2, .....
func DecodeRow(b []byte, cols map[int64]*types.FieldType, loc *time.Location) (map[int64]types.Datum, error) {
	return DecodeRowWithMapNew(b, cols, loc, nil)
}

// DecodeRowWithMapNew decode a row to datum map.
func DecodeRowWithMapNew(b []byte, cols map[int64]*types.FieldType, loc *time.Location, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	if row == nil {
		row = make(map[int64]types.Datum, len(cols))
	}
	if b == nil {
		return row, nil
	}
	if len(b) == 1 && b[0] == codec.NilFlag {
		return row, nil
	}

	reqCols := make([]rowcodec.ColInfo, len(cols))
	var idx int
	for id, tp := range cols {
		reqCols[idx] = rowcodec.ColInfo{
			ID:      id,
			Tp:      int32(tp.Tp),
			Flag:    int32(tp.Flag),
			Flen:    tp.Flen,
			Decimal: tp.Decimal,
			Elems:   tp.Elems,
		}
		idx++
	}
	// for decodeToMap:
	// - no need handle
	// - no need get default value
	rd := rowcodec.NewDatumMapDecoder(reqCols, -1, loc)
	return rd.DecodeToDatumMap(b, -1, row)
}

// unflatten converts a raw datum to a column datum.
func unflatten(datum types.Datum, ft *types.FieldType, loc *time.Location) (types.Datum, error) {
	return datum, nil
}

// CutIndexKey cuts encoded index key into colIDs to bytes slices map.
// The returned value b is the remaining bytes of the key which would be empty if it is unique index or handle data
// if it is non-unique index.
func CutIndexKey(key kv.Key, colIDs []int64) (values map[int64][]byte, b []byte, err error) {
	b = key[prefixLen+idLen:]
	values = make(map[int64][]byte)
	for _, id := range colIDs {
		var val []byte
		val, b, err = codec.CutOne(b)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		values[id] = val
	}
	return
}

// CutIndexPrefix cuts the index prefix.
func CutIndexPrefix(key kv.Key) []byte {
	return key[prefixLen+idLen:]
}

// CutIndexKeyNew cuts encoded index key into colIDs to bytes slices.
// The returned value b is the remaining bytes of the key which would be empty if it is unique index or handle data
// if it is non-unique index.
func CutIndexKeyNew(key kv.Key, length int) (values [][]byte, b []byte, err error) {
	b = key[prefixLen+idLen:]
	values = make([][]byte, 0, length)
	for i := 0; i < length; i++ {
		var val []byte
		val, b, err = codec.CutOne(b)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		values = append(values, val)
	}
	return
}

// PrimaryKeyStatus is the primary key column status.
type PrimaryKeyStatus int

const (
	// PrimaryKeyNotExists means no need to decode primary key column value when DecodeIndexKV.
	PrimaryKeyNotExists PrimaryKeyStatus = iota
	// PrimaryKeyIsSigned means decode primary key column value as int64 when DecodeIndexKV.
	PrimaryKeyIsSigned
	// PrimaryKeyIsUnsigned means decode primary key column value as uint64 when DecodeIndexKV.
	PrimaryKeyIsUnsigned
)

// DecodeIndexKV uses to decode index key values.
func DecodeIndexKV(key, value []byte, colsLen int, pkStatus PrimaryKeyStatus) ([][]byte, error) {
	values, b, err := CutIndexKeyNew(key, colsLen)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(b) > 0 {
		if pkStatus != PrimaryKeyNotExists {
			values = append(values, b)
		}
	} else if pkStatus != PrimaryKeyNotExists {
		handle, err := DecodeIndexValueAsHandle(value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		var handleDatum types.Datum
		if pkStatus == PrimaryKeyIsUnsigned {
			handleDatum = types.NewUintDatum(uint64(handle))
		} else {
			handleDatum = types.NewIntDatum(handle)
		}
		handleBytes := make([]byte, 0, 8)
		handleBytes, err = codec.EncodeValue(nil, handleBytes, handleDatum)
		if err != nil {
			return nil, errors.Trace(err)
		}
		values = append(values, handleBytes)
	}
	return values, nil
}

// DecodeIndexHandle uses to decode the handle from index key/value.
func DecodeIndexHandle(key, value []byte, colsLen int, pkTp *types.FieldType) (int64, error) {
	_, b, err := CutIndexKeyNew(key, colsLen)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(b) > 0 {
		d, err := DecodeColumnValue(b, pkTp, nil)
		if err != nil {
			return 0, errors.Trace(err)
		}
		return d.GetInt64(), nil

	} else if len(value) >= 8 {
		return DecodeIndexValueAsHandle(value)
	}
	// Should never execute to here.
	return 0, errors.Errorf("no handle in index key: %v, value: %v", key, value)
}

// DecodeIndexValueAsHandle uses to decode index value as handle id.
func DecodeIndexValueAsHandle(data []byte) (int64, error) {
	var h int64
	buf := bytes.NewBuffer(data)
	err := binary.Read(buf, binary.BigEndian, &h)
	return h, errors.Trace(err)
}

// EncodeTableIndexPrefix encodes index prefix with tableID and idxID.
func EncodeTableIndexPrefix(tableID, idxID int64) kv.Key {
	key := make([]byte, 0, prefixLen)
	key = appendTableIndexPrefix(key, tableID)
	key = codec.EncodeInt(key, idxID)
	return key
}

// EncodeTablePrefix encodes table prefix with table ID.
func EncodeTablePrefix(tableID int64) kv.Key {
	var key kv.Key
	key = append(key, tablePrefix...)
	key = codec.EncodeInt(key, tableID)
	return key
}

// ReplaceRecordKeyTableID replace the tableID in the recordKey buf.
func ReplaceRecordKeyTableID(buf []byte, tableID int64) []byte {
	if len(buf) < len(tablePrefix)+8 {
		return buf
	}

	u := codec.EncodeIntToCmpUint(tableID)
	binary.BigEndian.PutUint64(buf[len(tablePrefix):], u)
	return buf
}

// GenTableRecordPrefix composes record prefix with tableID: "t[tableID]_r".
func GenTableRecordPrefix(tableID int64) kv.Key {
	buf := make([]byte, 0, len(tablePrefix)+8+len(recordPrefixSep))
	return appendTableRecordPrefix(buf, tableID)
}

// GenTableIndexPrefix composes index prefix with tableID: "t[tableID]_i".
func GenTableIndexPrefix(tableID int64) kv.Key {
	buf := make([]byte, 0, len(tablePrefix)+8+len(indexPrefixSep))
	return appendTableIndexPrefix(buf, tableID)
}

// IsIndexKey is used to check whether the key is an index key.
func IsIndexKey(k []byte) bool {
	return len(k) > 11 && k[0] == 't' && k[10] == 'i'
}

// IsUntouchedIndexKValue uses to check whether the key is index key, and the value is untouched,
// since the untouched index key/value is no need to commit.
func IsUntouchedIndexKValue(k, v []byte) bool {
	vLen := len(v)
	return IsIndexKey(k) &&
		((vLen == 1 || vLen == 9) && v[vLen-1] == kv.UnCommitIndexKVFlag)
}

// GenTablePrefix composes table record and index prefix: "t[tableID]".
func GenTablePrefix(tableID int64) kv.Key {
	buf := make([]byte, 0, len(tablePrefix)+8)
	buf = append(buf, tablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	return buf
}

// TruncateToRowKeyLen truncates the key to row key length if the key is longer than row key.
func TruncateToRowKeyLen(key kv.Key) kv.Key {
	if len(key) > RecordRowKeyLen {
		return key[:RecordRowKeyLen]
	}
	return key
}

// GetTableHandleKeyRange returns table handle's key range with tableID.
func GetTableHandleKeyRange(tableID int64) (startKey, endKey []byte) {
	startKey = EncodeRowKeyWithHandle(tableID, math.MinInt64)
	endKey = EncodeRowKeyWithHandle(tableID, math.MaxInt64)
	return
}

// GetTableIndexKeyRange returns table index's key range with tableID and indexID.
func GetTableIndexKeyRange(tableID, indexID int64) (startKey, endKey []byte) {
	startKey = EncodeIndexSeekKey(tableID, indexID, nil)
	endKey = EncodeIndexSeekKey(tableID, indexID, []byte{255})
	return
}

var (
	errInvalidKey       = terror.ClassXEval.New(mysql.ErrInvalidKey, mysql.MySQLErrName[mysql.ErrInvalidKey])
	errInvalidRecordKey = terror.ClassXEval.New(mysql.ErrInvalidRecordKey, mysql.MySQLErrName[mysql.ErrInvalidRecordKey])
	errInvalidIndexKey  = terror.ClassXEval.New(mysql.ErrInvalidIndexKey, mysql.MySQLErrName[mysql.ErrInvalidIndexKey])
)

func init() {
	mySQLErrCodes := map[terror.ErrCode]uint16{
		mysql.ErrInvalidKey:       mysql.ErrInvalidKey,
		mysql.ErrInvalidRecordKey: mysql.ErrInvalidRecordKey,
		mysql.ErrInvalidIndexKey:  mysql.ErrInvalidIndexKey,
	}
	terror.ErrClassToMySQLCodes[terror.ClassXEval] = mySQLErrCodes
}

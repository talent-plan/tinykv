package rowcodec

import (
	"math"
	"sort"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

// XRowBuilder is used to build new row from an old row.
type XRowBuilder struct {
	row        XRow
	sc         *stmtctx.StatementContext
	tempColIDs []int64
	values     []types.Datum
	tempData   []byte
}

func (rb *XRowBuilder) SetOldRow(oldRow []byte) error {
	for len(oldRow) > 1 {
		var d types.Datum
		var err error
		oldRow, d, err = codec.DecodeOne(oldRow)
		if err != nil {
			return errors.Trace(err)
		}
		colID := d.GetInt64()
		oldRow, d, err = codec.DecodeOne(oldRow)
		if err != nil {
			return errors.Trace(err)
		}
		if colID > 255 {
			rb.row.large = true
		}
		if d.IsNull() {
			rb.row.numNullCols++
		} else {
			rb.row.numNotNullCols++
		}
		rb.tempColIDs = append(rb.tempColIDs, colID)
		rb.values = append(rb.values, d)
	}
	return nil
}

func (rb *XRowBuilder) Build(buf []byte) ([]byte, error) {
	// Separate null and not-null column IDs.
	nullIdx := len(rb.tempColIDs) - int(rb.row.numNullCols)
	notNullIdx := 0
	if rb.row.large {
		rb.row.colIDs32 = make([]uint32, len(rb.tempColIDs))
	} else {
		rb.row.colIDs = make([]byte, len(rb.tempColIDs))
	}
	for i, colID := range rb.tempColIDs {
		if rb.values[i].IsNull() {
			if rb.row.large {
				rb.row.colIDs32[nullIdx] = uint32(colID)
			} else {
				rb.row.colIDs[nullIdx] = byte(colID)
			}
			nullIdx++
		} else {
			if rb.row.large {
				rb.row.colIDs32[notNullIdx] = uint32(colID)
			} else {
				rb.row.colIDs[notNullIdx] = byte(colID)
			}
			rb.values[notNullIdx] = rb.values[i]
			notNullIdx++
		}
	}
	sort.Sort(rb)
	if rb.row.large {
		sort.Slice(rb.row.colIDs32[notNullIdx:], func(i, j int) bool {
			return rb.row.colIDs32[i] < rb.row.colIDs32[j]
		})
	} else {
		sort.Slice(rb.row.colIDs[notNullIdx:], func(i, j int) bool {
			return rb.row.colIDs[i] < rb.row.colIDs[j]
		})
	}
	for i := 0; i < notNullIdx; i++ {
		d := rb.values[i]
		switch d.Kind() {
		case types.KindInt64:
			rb.row.valFlags = append(rb.row.valFlags, IntFlag)
			binVal := encodeInt(d.GetInt64())
			rb.row.data = append(rb.row.data, binVal...)
		case types.KindUint64:
			rb.row.valFlags = append(rb.row.valFlags, UintFlag)
			binVal := encodeUint(d.GetUint64())
			rb.row.data = append(rb.row.data, binVal...)
		case types.KindString, types.KindBytes:
			rb.row.valFlags = append(rb.row.valFlags, BytesFlag)
			rb.row.data = append(rb.row.data, d.GetBytes()...)
		default:
			var err error
			rb.tempData, err = codec.EncodeValue(rb.sc, rb.tempData[:0], d)
			if err != nil {
				return nil, errors.Trace(err)
			}
			rb.row.valFlags = append(rb.row.valFlags, rb.tempData[0])
			rb.row.data = append(rb.row.data, rb.tempData[1:]...)
		}
		rb.row.offsets32 = append(rb.row.offsets32, uint32(len(rb.row.data)))
		if len(rb.row.data) > math.MaxUint16 && !rb.row.large {
			rb.row.colIDs32 = make([]uint32, notNullIdx)
			rb.row.offsets32 = make([]uint32, notNullIdx)
			for i := 0; i < notNullIdx; i++ {
				rb.row.colIDs32[i] = uint32(rb.row.colIDs[i])
				rb.row.offsets32[i] = uint32(rb.row.offsets[i])
			}
			rb.row.large = true
		}
		if rb.row.large {
			rb.row.offsets32 = append(rb.row.offsets32, uint32(len(rb.row.data)))
		} else {
			rb.row.offsets = append(rb.row.offsets, uint16(len(rb.row.data)))
		}
	}
	if !rb.row.large {
		if len(rb.row.data) >= math.MaxUint16 {
			rb.row.large = true
			rb.row.colIDs32 = make([]uint32, len(rb.row.colIDs))
			for i, val := range rb.row.colIDs {
				rb.row.colIDs32[i] = uint32(val)
			}
		} else {
			rb.row.offsets = make([]uint16, len(rb.row.offsets32))
			for i, val := range rb.row.offsets32 {
				rb.row.offsets[i] = uint16(val)
			}
		}
	}
	buf = append(buf, CodecVer)
	flag := byte(0)
	if rb.row.large {
		flag = 1
	}
	buf = append(buf, flag)
	buf = append(buf, byte(rb.row.numNotNullCols), byte(rb.row.numNotNullCols>>8))
	buf = append(buf, byte(rb.row.numNullCols), byte(rb.row.numNullCols>>8))
	buf = append(buf, rb.row.valFlags...)
	if rb.row.large {
		buf = append(buf, u32SliceToBytes(rb.row.colIDs32)...)
		buf = append(buf, u32SliceToBytes(rb.row.offsets32)...)
	} else {
		buf = append(buf, rb.row.colIDs...)
		buf = append(buf, u16SliceToBytes(rb.row.offsets)...)
	}
	buf = append(buf, rb.row.data...)
	return buf, nil
}

func (rb *XRowBuilder) Less(i, j int) bool {
	if rb.row.large {
		return rb.row.colIDs32[i] < rb.row.colIDs32[j]
	}
	return rb.row.colIDs[i] < rb.row.colIDs[j]
}

func (rb *XRowBuilder) Len() int {
	return len(rb.tempColIDs) - int(rb.row.numNullCols)
}

func (rb *XRowBuilder) Swap(i, j int) {
	if rb.row.large {
		rb.row.colIDs32[i], rb.row.colIDs32[j] = rb.row.colIDs32[j], rb.row.colIDs32[i]
	} else {
		rb.row.colIDs[i], rb.row.colIDs[j] = rb.row.colIDs[j], rb.row.colIDs[i]
	}
	rb.values[i], rb.values[j] = rb.values[j], rb.values[i]
}

var defaultStmtCtx = &stmtctx.StatementContext{
	TimeZone: time.Local,
}

const (
	rowKeyLen       = 19
	recordPrefixIdx = 10
)

func IsRowKey(key []byte) bool {
	return len(key) == rowKeyLen && key[0] == 't' && key[recordPrefixIdx] == 'r'
}

// OldRowToXRow converts old row to new row.
func OldRowToXRow(oldRow, buf []byte) ([]byte, error) {
	var builder XRowBuilder
	builder.sc = defaultStmtCtx
	err := builder.SetOldRow(oldRow)
	if err != nil {
		return nil, err
	}
	return builder.Build(buf[:0])
}

// XRowToOldRow converts new row to old row.
func XRowToOldRow(rowData, buf []byte) ([]byte, error) {
	if len(rowData) == 0 {
		return rowData, nil
	}
	buf = buf[:0]
	var r XRow
	err := r.setRowData(rowData)
	if err != nil {
		return nil, err
	}
	for i, colID := range r.colIDs {
		buf = append(buf, VarintFlag)
		buf = codec.EncodeVarint(buf, int64(colID))
		if i < int(r.numNotNullCols) {
			val := r.getData(i)
			switch r.valFlags[i] {
			case BytesFlag:
				buf = append(buf, CompactBytesFlag)
				buf = codec.EncodeCompactBytes(buf, val)
			case IntFlag:
				buf = append(buf, IntFlag)
				buf = codec.EncodeInt(buf, decodeInt(val))
			case UintFlag:
				buf = append(buf, UintFlag)
				buf = codec.EncodeUint(buf, decodeUint(val))
			default:
				buf = append(buf, r.valFlags[i])
				buf = append(buf, val...)
			}
		} else {
			buf = append(buf, NilFlag)
		}
	}
	if len(buf) == 0 {
		buf = append(buf, NilFlag)
	}
	return buf, nil
}

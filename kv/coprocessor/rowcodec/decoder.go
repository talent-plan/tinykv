package rowcodec

import (
	"math"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
)

// Decoder decodes the row to chunk.Chunk.
type Decoder struct {
	row
	requestColIDs []int64
	handleColID   int64
	requestTypes  []*types.FieldType
	origDefaults  [][]byte
	loc           *time.Location
}

// NewDecoder creates a NewDecoder.
// requestColIDs is the columnIDs to decode. tps is the field types for request columns.
// origDefault is the original default value in old format, if the column ID is not found in the row,
// the origDefault will be used.
func NewDecoder(requestColIDs []int64, handleColID int64, tps []*types.FieldType, origDefaults [][]byte,
	loc *time.Location) (*Decoder, error) {
	xOrigDefaultVals := make([][]byte, len(origDefaults))
	for i := 0; i < len(origDefaults); i++ {
		if len(origDefaults[i]) == 0 {
			continue
		}
		xDefaultVal, err := convertDefaultValue(origDefaults[i])
		if err != nil {
			return nil, err
		}
		xOrigDefaultVals[i] = xDefaultVal
	}
	return &Decoder{
		requestColIDs: requestColIDs,
		handleColID:   handleColID,
		requestTypes:  tps,
		origDefaults:  xOrigDefaultVals,
		loc:           loc,
	}, nil
}

func convertDefaultValue(defaultVal []byte) (colVal []byte, err error) {
	var d types.Datum
	_, d, err = codec.DecodeOne(defaultVal)
	if err != nil {
		return
	}
	switch d.Kind() {
	case types.KindNull:
		return nil, nil
	case types.KindInt64:
		return encodeInt(nil, d.GetInt64()), nil
	case types.KindUint64:
		return encodeUint(nil, d.GetUint64()), nil
	case types.KindString, types.KindBytes:
		return d.GetBytes(), nil
	case types.KindFloat32:
		return encodeUint(nil, uint64(math.Float32bits(d.GetFloat32()))), nil
	case types.KindFloat64:
		return encodeUint(nil, math.Float64bits(d.GetFloat64())), nil
	default:
		return defaultVal[1:], nil
	}
}

// Decode decodes a row to chunk.
func (decoder *Decoder) Decode(rowData []byte, handle int64, chk *chunk.Chunk) error {
	err := decoder.setRowData(rowData)
	if err != nil {
		return err
	}
	for colIdx, colID := range decoder.requestColIDs {
		if colID == decoder.handleColID {
			chk.AppendInt64(colIdx, handle)
			continue
		}
		// Search the column in not-null columns array.
		i, j := 0, int(decoder.numNotNullCols)
		var found bool
		for i < j {
			h := int(uint(i+j) >> 1) // avoid overflow when computing h
			// i ≤ h < j
			var v int64
			if decoder.large {
				v = int64(decoder.colIDs32[h])
			} else {
				v = int64(decoder.colIDs[h])
			}
			if v < colID {
				i = h + 1
			} else if v > colID {
				j = h
			} else {
				found = true
				colData := decoder.getData(h)
				err := decoder.decodeColData(colIdx, colData, chk)
				if err != nil {
					return err
				}
				break
			}
		}
		if found {
			continue
		}
		defaultVal := decoder.origDefaults[colIdx]
		if decoder.isNull(colID, defaultVal) {
			chk.AppendNull(colIdx)
		} else {
			err := decoder.decodeColData(colIdx, defaultVal, chk)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ColumnIsNull returns if the column value is null. Mainly used for count column aggregation.
func (decoder *Decoder) ColumnIsNull(rowData []byte, colID int64, defaultVal []byte) (bool, error) {
	err := decoder.setRowData(rowData)
	if err != nil {
		return false, err
	}
	// Search the column in not-null columns array.
	i, j := 0, int(decoder.numNotNullCols)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		var v int64
		if decoder.large {
			v = int64(decoder.colIDs32[h])
		} else {
			v = int64(decoder.colIDs[h])
		}
		if v < colID {
			i = h + 1
		} else if v > colID {
			j = h
		} else {
			return false, nil
		}
	}
	return decoder.isNull(colID, defaultVal), nil
}

func (decoder *Decoder) isNull(colID int64, defaultVal []byte) bool {
	// Search the column in null columns array.
	i, j := int(decoder.numNotNullCols), int(decoder.numNotNullCols+decoder.numNullCols)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		var v int64
		if decoder.large {
			v = int64(decoder.colIDs32[h])
		} else {
			v = int64(decoder.colIDs[h])
		}
		if v < colID {
			i = h + 1
		} else if v > colID {
			j = h
		} else {
			return true
		}
	}
	return defaultVal == nil
}

func (decoder *Decoder) decodeColData(colIdx int, colData []byte, chk *chunk.Chunk) error {
	ft := decoder.requestTypes[colIdx]
	switch ft.Tp {
	case mysql.TypeLonglong, mysql.TypeLong, mysql.TypeInt24, mysql.TypeShort, mysql.TypeTiny, mysql.TypeYear:
		if mysql.HasUnsignedFlag(ft.Flag) {
			chk.AppendUint64(colIdx, decodeUint(colData))
		} else {
			chk.AppendInt64(colIdx, decodeInt(colData))
		}
	case mysql.TypeFloat:
		_, fVal, err := codec.DecodeFloat(colData)
		if err != nil {
			return err
		}
		chk.AppendFloat32(colIdx, float32(fVal))
	case mysql.TypeDouble:
		_, fVal, err := codec.DecodeFloat(colData)
		if err != nil {
			return err
		}
		chk.AppendFloat64(colIdx, fVal)
	case mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		chk.AppendBytes(colIdx, colData)
	case mysql.TypeBit:
		byteSize := (ft.Flen + 7) >> 3
		chk.AppendBytes(colIdx, types.NewBinaryLiteralFromUint(decodeUint(colData), byteSize))
	default:
		return errors.Errorf("unknown type %d", ft.Tp)
	}
	return nil
}

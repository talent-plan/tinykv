package rowcodec

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct{}

func (s *testSuite) TestRowCodec(c *C) {
	colIDs := []int64{1, 2, 3}
	tps := make([]*types.FieldType, 3)
	for i := 0; i < 3; i++ {
		tps[i] = types.NewFieldType(mysql.TypeLonglong)
	}
	sc := new(stmtctx.StatementContext)
	oldRow, err := tablecodec.EncodeRow(sc, types.MakeDatums(1, 2, 3), colIDs, nil, nil)
	c.Check(err, IsNil)

	var rb Encoder
	newRow, err := rb.EncodeFromOldRow(oldRow, nil)
	c.Check(err, IsNil)
	rd, err := NewDecoder(colIDs, 0, tps, make([][]byte, 3), time.Local)
	c.Assert(err, IsNil)
	chk := chunk.NewChunkWithCapacity(tps, 1)
	err = rd.Decode(newRow, -1, chk)
	c.Assert(err, IsNil)
	row := chk.GetRow(0)
	for i := 0; i < 3; i++ {
		c.Assert(row.GetInt64(i), Equals, int64(i)+1)
	}
}

func (s *testSuite) TestRowCodecIsNull(c *C) {
	colIDs := []int64{1, 2}
	tps := make([]*types.FieldType, 2)
	for i := 0; i < 2; i++ {
		tps[i] = types.NewFieldType(mysql.TypeLonglong)
	}
	var rb Encoder
	newRow, err := rb.Encode(colIDs, types.MakeDatums(1, nil), nil)
	c.Assert(err, IsNil)
	rd, err := NewDecoder(colIDs, 0, tps, make([][]byte, 3), time.Local)
	c.Assert(err, IsNil)
	defaultVal := make([]byte, 1)
	isNull, err := rd.ColumnIsNull(newRow, 1, defaultVal)
	c.Assert(err, IsNil)
	c.Assert(isNull, IsFalse)
	isNull, err = rd.ColumnIsNull(newRow, 1, nil)
	c.Assert(err, IsNil)
	c.Assert(isNull, IsFalse)
	isNull, err = rd.ColumnIsNull(newRow, 2, defaultVal)
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	isNull, err = rd.ColumnIsNull(newRow, 3, defaultVal)
	c.Assert(err, IsNil)
	c.Assert(isNull, IsFalse)
	isNull, err = rd.ColumnIsNull(newRow, 3, nil)
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
}

func BenchmarkEncode(b *testing.B) {
	b.ReportAllocs()
	oldRow := types.MakeDatums(1, "abc", 1.1)
	var xb Encoder
	var buf []byte
	colIDs := []int64{1, 2, 3}
	var err error
	for i := 0; i < b.N; i++ {
		buf, err = xb.Encode(colIDs, oldRow, buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeFromOldRow(b *testing.B) {
	b.ReportAllocs()
	oldRow := types.MakeDatums(1, "abc", 1.1)
	oldRowData, err := tablecodec.EncodeRow(new(stmtctx.StatementContext), oldRow, []int64{1, 2, 3}, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	var xb Encoder
	var buf []byte
	for i := 0; i < b.N; i++ {
		buf, err = xb.EncodeFromOldRow(oldRowData, buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	b.ReportAllocs()
	oldRow := types.MakeDatums(1, "abc", 1.1)
	colIDs := []int64{-1, 2, 3}
	tps := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeDouble),
	}
	var xb Encoder
	xRowData, err := xb.Encode(colIDs, oldRow, nil)
	if err != nil {
		b.Fatal(err)
	}
	decoder, err := NewDecoder(colIDs, -1, tps, make([][]byte, 3), time.Local)
	if err != nil {
		b.Fatal(err)
	}
	chk := chunk.NewChunkWithCapacity(tps, 1)
	for i := 0; i < b.N; i++ {
		chk.Reset()
		err = decoder.Decode(xRowData, 1, chk)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIsNull(b *testing.B) {
	b.ReportAllocs()
	oldRow := types.MakeDatums(1, "abc", 1.1)
	colIDs := []int64{-1, 2, 3}
	tps := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeDouble),
	}
	var xb Encoder
	xRowData, err := xb.Encode(colIDs, oldRow, nil)
	if err != nil {
		b.Fatal(err)
	}
	decoder, err := NewDecoder(colIDs, -1, tps, make([][]byte, 3), time.Local)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		_, err = decoder.ColumnIsNull(xRowData, int64(i)%4, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

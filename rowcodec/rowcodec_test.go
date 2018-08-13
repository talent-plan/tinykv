package rowcodec

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
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

	rb := new(XRowBuilder)
	rb.SetOldRow(oldRow)

	newRow, err := rb.Build(nil)
	c.Check(err, IsNil)
	rd, err := NewXRowDecoder(colIDs, 0, tps, make([][]byte, 3), time.Local)
	c.Assert(err, IsNil)
	chk := chunk.NewChunkWithCapacity(tps, 1)
	err = rd.Decode(newRow, -1, chk)
	c.Assert(err, IsNil)
	row := chk.GetRow(0)
	for i := 0; i < 3; i++ {
		c.Assert(row.GetInt64(i), Equals, int64(i)+1)
	}
}

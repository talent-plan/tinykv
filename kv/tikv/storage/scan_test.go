package storage

import (
	"testing"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

// TestScanEmpty tests a scan after the end of the DB.
func TestScanEmpty(t *testing.T) {
	builder := builderForScan(t)

	cmd := NewScan(builder.scanRequest([]byte{200}, 10000))
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.ScanResponse)
	assert.Nil(t, resp.RegionError)
	assert.Empty(t, resp.Pairs)
}

// TestScanLimitZero tests we get nothing if limit is 0.
func TestScanLimitZero(t *testing.T) {
	builder := builderForScan(t)

	cmd := NewScan(builder.scanRequest([]byte{3}, 0))
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.ScanResponse)
	assert.Nil(t, resp.RegionError)
	assert.Empty(t, resp.Pairs)
}

// TestScanAll start at the beginning of the DB and read all pairs, respecting the timestamp.
func TestScanAll(t *testing.T) {
	builder := builderForScan(t)

	cmd := NewScan(builder.scanRequest([]byte{0}, 10000))
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.ScanResponse)

	assert.Nil(t, resp.RegionError)
	assert.Equal(t, 11, len(resp.Pairs))
	assert.Equal(t, []byte{1}, resp.Pairs[0].Key)
	assert.Equal(t, []byte{50}, resp.Pairs[0].Value)
	assert.Equal(t, []byte{199}, resp.Pairs[10].Key)
	assert.Equal(t, []byte{54}, resp.Pairs[10].Value)
}

// TestScanLimit tests that scan takes the limit into account.
func TestScanLimit(t *testing.T) {
	builder := builderForScan(t)

	cmd := NewScan(builder.scanRequest([]byte{2}, 6))
	resp := builder.runOneCmd(&cmd).(*kvrpcpb.ScanResponse)

	assert.Nil(t, resp.RegionError)
	assert.Equal(t, 6, len(resp.Pairs))
	assert.Equal(t, []byte{3}, resp.Pairs[0].Key)
	assert.Equal(t, []byte{51}, resp.Pairs[0].Value)
	assert.Equal(t, []byte{4}, resp.Pairs[5].Key)
	assert.Equal(t, []byte{52}, resp.Pairs[5].Value)
}

// TestScanDeleted scan over a value which is deleted then replaced.
func TestScanDeleted(t *testing.T) {
	builder := builderForScan(t)

	req1 := builder.scanRequest([]byte{100}, 10000)
	req1.Version = 100
	cmd1 := NewScan(req1)
	req2 := builder.scanRequest([]byte{100}, 10000)
	req2.Version = 105
	cmd2 := NewScan(req2)
	req3 := builder.scanRequest([]byte{100}, 10000)
	req3.Version = 120
	cmd3 := NewScan(req3)

	resps := builder.runCommands(&cmd1, &cmd2, &cmd3)

	resp1 := resps[0].(*kvrpcpb.ScanResponse)
	assert.Nil(t, resp1.RegionError)
	assert.Equal(t, 3, len(resp1.Pairs))
	assert.Equal(t, []byte{150}, resp1.Pairs[1].Key)
	assert.Equal(t, []byte{42}, resp1.Pairs[1].Value)

	resp2 := resps[1].(*kvrpcpb.ScanResponse)
	assert.Nil(t, resp2.RegionError)
	assert.Equal(t, 2, len(resp2.Pairs))
	assert.Equal(t, []byte{120}, resp2.Pairs[0].Key)
	assert.Equal(t, []byte{199}, resp2.Pairs[1].Key)

	resp3 := resps[2].(*kvrpcpb.ScanResponse)
	assert.Nil(t, resp3.RegionError)
	assert.Equal(t, 3, len(resp3.Pairs))
	assert.Equal(t, []byte{150}, resp3.Pairs[1].Key)
	assert.Equal(t, []byte{64}, resp3.Pairs[1].Value)
}

func builderForScan(t *testing.T) *testBuilder {
	values := []kv{
		// Committed before 100.
		{engine_util.CfDefault, []byte{1}, 80, []byte{50}},
		{engine_util.CfWrite, []byte{1}, 99, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{engine_util.CfDefault, []byte{1, 23}, 80, []byte{55}},
		{engine_util.CfWrite, []byte{1, 23}, 99, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{engine_util.CfDefault, []byte{3}, 80, []byte{51}},
		{engine_util.CfWrite, []byte{3}, 99, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{engine_util.CfDefault, []byte{3, 45}, 80, []byte{56}},
		{engine_util.CfWrite, []byte{3, 45}, 99, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{engine_util.CfDefault, []byte{3, 46}, 80, []byte{57}},
		{engine_util.CfWrite, []byte{3, 46}, 99, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{engine_util.CfDefault, []byte{3, 47}, 80, []byte{58}},
		{engine_util.CfWrite, []byte{3, 47}, 99, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{engine_util.CfDefault, []byte{3, 48}, 80, []byte{59}},
		{engine_util.CfWrite, []byte{3, 48}, 99, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{engine_util.CfDefault, []byte{4}, 80, []byte{52}},
		{engine_util.CfWrite, []byte{4}, 99, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{engine_util.CfDefault, []byte{120}, 80, []byte{53}},
		{engine_util.CfWrite, []byte{120}, 99, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{engine_util.CfDefault, []byte{199}, 80, []byte{54}},
		{engine_util.CfWrite, []byte{199}, 99, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},

		// Committed after 100.
		{engine_util.CfDefault, []byte{4, 45}, 110, []byte{58}},
		{engine_util.CfWrite, []byte{4, 45}, 116, []byte{1, 0, 0, 0, 0, 0, 0, 0, 110}},
		{engine_util.CfDefault, []byte{4, 46}, 110, []byte{57}},
		{engine_util.CfWrite, []byte{4, 46}, 116, []byte{1, 0, 0, 0, 0, 0, 0, 0, 110}},
		{engine_util.CfDefault, []byte{4, 47}, 110, []byte{58}},
		{engine_util.CfWrite, []byte{4, 47}, 116, []byte{1, 0, 0, 0, 0, 0, 0, 0, 110}},
		{engine_util.CfDefault, []byte{4, 48}, 110, []byte{59}},
		{engine_util.CfWrite, []byte{4, 48}, 116, []byte{1, 0, 0, 0, 0, 0, 0, 0, 110}},

		// Committed after 100, but started before.
		{engine_util.CfDefault, []byte{5, 45}, 97, []byte{60}},
		{engine_util.CfWrite, []byte{5, 45}, 101, []byte{1, 0, 0, 0, 0, 0, 0, 0, 97}},
		{engine_util.CfDefault, []byte{5, 46}, 97, []byte{61}},
		{engine_util.CfWrite, []byte{5, 46}, 101, []byte{1, 0, 0, 0, 0, 0, 0, 0, 97}},
		{engine_util.CfDefault, []byte{5, 47}, 97, []byte{62}},
		{engine_util.CfWrite, []byte{5, 47}, 101, []byte{1, 0, 0, 0, 0, 0, 0, 0, 97}},
		{engine_util.CfDefault, []byte{5, 48}, 97, []byte{63}},
		{engine_util.CfWrite, []byte{5, 48}, 101, []byte{1, 0, 0, 0, 0, 0, 0, 0, 97}},

		// A deleted value and replaced value.
		{engine_util.CfDefault, []byte{150}, 80, []byte{42}},
		{engine_util.CfWrite, []byte{150}, 99, []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{engine_util.CfWrite, []byte{150}, 101, []byte{2, 0, 0, 0, 0, 0, 0, 0, 97}},
		{engine_util.CfDefault, []byte{150}, 110, []byte{64}},
		{engine_util.CfWrite, []byte{150}, 116, []byte{1, 0, 0, 0, 0, 0, 0, 0, 110}},
	}
	builder := newBuilder(t)
	builder.init(values)
	return &builder
}

func (builder *testBuilder) scanRequest(startKey []byte, limit uint32) *kvrpcpb.ScanRequest {
	var req kvrpcpb.ScanRequest
	req.StartKey = startKey
	req.Limit = limit
	req.Version = builder.nextTs()
	return &req
}

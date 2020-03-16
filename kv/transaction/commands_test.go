package transaction

// This file contains utility code for testing commands.

import (
	"context"
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/pingcap-incubator/tinykv/kv/server"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

// testBuilder is a helper type for running command tests.
type testBuilder struct {
	t      *testing.T
	server *server.Server
	// mem will always be the backing store for server.
	mem *storage.MemStorage
	// Keep track of timestamps.
	prevTs uint64
}

// kv is a type which identifies a key/value pair to testBuilder.
type kv struct {
	cf string
	// The user key (unencoded, no time stamp).
	key []byte
	// Can be elided. The builder's prevTS will be used if the ts is needed.
	ts uint64
	// Can be elided in assertion functions. If elided then testBuilder checks that the value has not changed.
	value []byte
}

func newBuilder(t *testing.T) testBuilder {
	mem := storage.NewMemStorage()
	server := server.NewServer(mem)
	server.Latches.Validation = func(txn *mvcc.MvccTxn, keys [][]byte) {
		keyMap := make(map[string]struct{})
		for _, k := range keys {
			keyMap[string(k)] = struct{}{}
		}
		for _, wr := range txn.Writes() {
			key := wr.Key()
			// This is a bit of a hack and relies on all the raw tests using keys shorter than 9 bytes, which is the
			// minimum length for an encoded key.
			if len(key) > 8 {
				switch wr.Cf() {
				case engine_util.CfDefault:
					key = mvcc.DecodeUserKey(wr.Key())
				case engine_util.CfWrite:
					key = mvcc.DecodeUserKey(wr.Key())
				}
			}
			if _, ok := keyMap[string(key)]; !ok {
				t.Errorf("Failed latching validation: tried to write a key which was not latched in %v", wr.Data)
			}
		}
	}
	return testBuilder{t, server, mem, 99}
}

// init sets values in the test's DB.
func (builder *testBuilder) init(values []kv) {
	for _, kv := range values {
		ts := kv.ts
		if ts == 0 {
			ts = builder.prevTs
		}
		switch kv.cf {
		case engine_util.CfDefault:
			builder.mem.Set(kv.cf, mvcc.EncodeKey(kv.key, ts), kv.value)
		case engine_util.CfWrite:
			builder.mem.Set(kv.cf, mvcc.EncodeKey(kv.key, ts), kv.value)
		case engine_util.CfLock:
			builder.mem.Set(kv.cf, kv.key, kv.value)
		}
	}
}

func (builder *testBuilder) runRequests(reqs ...interface{}) []interface{} {
	var result []interface{}
	for _, req := range reqs {
		reqName := fmt.Sprintf("%v", reflect.TypeOf(req))
		reqName = strings.TrimPrefix(strings.TrimSuffix(reqName, "Request"), "*kvrpcpb.")
		fnName := "Kv" + reqName
		serverVal := reflect.ValueOf(builder.server)
		fn := serverVal.MethodByName(fnName)
		ctxtVal := reflect.ValueOf(context.Background())
		reqVal := reflect.ValueOf(req)

		results := fn.Call([]reflect.Value{ctxtVal, reqVal})

		assert.Nil(builder.t, results[1].Interface())
		result = append(result, results[0].Interface())
	}
	return result
}

// runOneCmd is like runCommands but only runs a single command.
func (builder *testBuilder) runOneRequest(req interface{}) interface{} {
	return builder.runRequests(req)[0]
}

func (builder *testBuilder) nextTs() uint64 {
	builder.prevTs++
	return builder.prevTs
}

// ts returns the most recent timestamp used by testBuilder as a byte.
func (builder *testBuilder) ts() byte {
	return byte(builder.prevTs)
}

// assert that a key/value pair exists and has the given value, or if there is no value that it is unchanged.
func (builder *testBuilder) assert(kvs []kv) {
	for _, kv := range kvs {
		var key []byte
		ts := kv.ts
		if ts == 0 {
			ts = builder.prevTs
		}
		switch kv.cf {
		case engine_util.CfDefault:
			key = mvcc.EncodeKey(kv.key, ts)
		case engine_util.CfWrite:
			key = mvcc.EncodeKey(kv.key, ts)
		case engine_util.CfLock:
			key = kv.key
		}
		if kv.value == nil {
			assert.False(builder.t, builder.mem.HasChanged(kv.cf, key))
		} else {
			assert.Equal(builder.t, kv.value, builder.mem.Get(kv.cf, key))
		}
	}
}

// assertLen asserts the size of one of the column families.
func (builder *testBuilder) assertLen(cf string, size int) {
	assert.Equal(builder.t, size, builder.mem.Len(cf))
}

// assertLens asserts the size of each column family.
func (builder *testBuilder) assertLens(def int, lock int, write int) {
	builder.assertLen(engine_util.CfDefault, def)
	builder.assertLen(engine_util.CfLock, lock)
	builder.assertLen(engine_util.CfWrite, write)
}

func (builder *testBuilder) prewriteRequest(muts ...*kvrpcpb.Mutation) *kvrpcpb.PrewriteRequest {
	var req kvrpcpb.PrewriteRequest
	req.PrimaryLock = []byte{1}
	req.StartVersion = builder.nextTs()
	req.Mutations = muts
	return &req
}

func mutation(key byte, value []byte, op kvrpcpb.Op) *kvrpcpb.Mutation {
	var mut kvrpcpb.Mutation
	mut.Key = []byte{key}
	mut.Value = value
	mut.Op = op
	return &mut
}

func (builder *testBuilder) commitRequest(keys ...[]byte) *kvrpcpb.CommitRequest {
	var req kvrpcpb.CommitRequest
	req.StartVersion = builder.nextTs()
	req.CommitVersion = builder.prevTs + 10
	req.Keys = keys
	return &req
}

func (builder *testBuilder) rollbackRequest(keys ...[]byte) *kvrpcpb.BatchRollbackRequest {
	var req kvrpcpb.BatchRollbackRequest
	req.StartVersion = builder.nextTs()
	req.Keys = keys
	return &req
}

func (builder *testBuilder) checkTxnStatusRequest(key []byte) *kvrpcpb.CheckTxnStatusRequest {
	var req kvrpcpb.CheckTxnStatusRequest
	builder.nextTs()
	req.LockTs = binary.BigEndian.Uint64([]byte{0, 0, 5, 0, 0, 0, 0, builder.ts()})
	req.CurrentTs = binary.BigEndian.Uint64([]byte{0, 0, 6, 0, 0, 0, 0, builder.ts()})
	req.PrimaryKey = key
	return &req
}

func resolveRequest(startTs uint64, commitTs uint64) *kvrpcpb.ResolveLockRequest {
	var req kvrpcpb.ResolveLockRequest
	req.StartVersion = startTs
	req.CommitVersion = commitTs
	return &req
}

func (builder *testBuilder) scanRequest(startKey []byte, limit uint32) *kvrpcpb.ScanRequest {
	var req kvrpcpb.ScanRequest
	req.StartKey = startKey
	req.Limit = limit
	req.Version = builder.nextTs()
	return &req
}

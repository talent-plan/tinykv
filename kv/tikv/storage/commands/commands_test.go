package commands

// This file contains utility code for testing commands.

import (
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/exec"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/interfaces"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/kvstore"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/stretchr/testify/assert"
	"testing"
)

// testBuilder is a helper type for running command tests.
type testBuilder struct {
	t      *testing.T
	sched  *exec.Scheduler
	mem    *inner_server.MemInnerServer
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
	mem := inner_server.NewMemInnerServer()
	sched := exec.NewScheduler(mem)
	sched.Latches.Validate = func(txn *kvstore.MvccTxn, keys [][]byte) {
		keyMap := make(map[string]struct{})
		for _, k := range keys {
			keyMap[string(k)] = struct{}{}
		}
		for _, wr := range txn.Writes {
			key := wr.Key()
			// This is a bit of a hack and relies on all the raw tests using keys shorter than 9 bytes, which is the
			// minimum length for an encoded key.
			if len(key) > 8 {
				switch wr.Cf() {
				case engine_util.CfDefault:
					key = kvstore.DecodeUserKey(wr.Key())
				case engine_util.CfWrite:
					key = kvstore.DecodeUserKey(wr.Key())
				}
			}
			if _, ok := keyMap[string(key)]; !ok {
				t.Errorf("Failed latching validation: tried to write a key which was not latched in %v", wr.Data)
			}
		}
	}
	return testBuilder{t, sched, mem, 99}
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
			builder.mem.Set(kv.cf, kvstore.EncodeKey(kv.key, ts), kv.value)
		case engine_util.CfWrite:
			builder.mem.Set(kv.cf, kvstore.EncodeKey(kv.key, ts), kv.value)
		case engine_util.CfLock:
			builder.mem.Set(kv.cf, kv.key, kv.value)
		}
	}
}

func (builder *testBuilder) runCommands(cmds ...interfaces.Command) []interface{} {
	var result []interface{}
	for _, c := range cmds {
		resp, err := builder.sched.Run(c)
		assert.Nil(builder.t, err)
		result = append(result, resp)
	}
	return result
}

// runOneCmd is like runCommands but only runs a single command.
func (builder *testBuilder) runOneCmd(cmd interfaces.Command) interface{} {
	return builder.runCommands(cmd)[0]
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
			key = kvstore.EncodeKey(kv.key, ts)
		case engine_util.CfWrite:
			key = kvstore.EncodeKey(kv.key, ts)
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

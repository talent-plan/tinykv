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
	sched  interfaces.Scheduler
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
	sched := exec.NewSeqScheduler(mem)
	return testBuilder{t, sched, mem, 99}
}

// init sets values in the test's DB.
func (t *testBuilder) init(values []kv) {
	for _, kv := range values {
		ts := kv.ts
		if ts == 0 {
			ts = t.prevTs
		}
		switch kv.cf {
		case engine_util.CfDefault:
			t.mem.Set(kv.cf, kvstore.EncodeKey(kv.key, ts), kv.value)
		case engine_util.CfWrite:
			t.mem.Set(kv.cf, kvstore.EncodeKey(kv.key, ts), kv.value)
		case engine_util.CfLock:
			t.mem.Set(kv.cf, kv.key, kv.value)
		}
	}
}

func (t *testBuilder) runCommands(cmds ...interfaces.Command) []interface{} {
	var result []interface{}
	for _, c := range cmds {
		ch := t.sched.Run(c)
		r := <-ch
		assert.Nil(t.t, r.Err)
		result = append(result, r.Response)
	}
	t.sched.Stop()
	return result
}

// runOneCmd is like runCommands but only runs a single command.
func (t *testBuilder) runOneCmd(cmd interfaces.Command) interface{} {
	return t.runCommands(cmd)[0]
}

func (t *testBuilder) nextTs() uint64 {
	t.prevTs++
	return t.prevTs
}

// ts returns the most recent timestamp used by testBuilder as a byte.
func (t *testBuilder) ts() byte {
	return byte(t.prevTs)
}

// assert that a key/value pair exists and has the given value, or if there is no value that it is unchanged.
func (t *testBuilder) assert(kvs []kv) {
	for _, kv := range kvs {
		var key []byte
		ts := kv.ts
		if ts == 0 {
			ts = t.prevTs
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
			assert.False(t.t, t.mem.HasChanged(kv.cf, key))
		} else {
			assert.Equal(t.t, kv.value, t.mem.Get(kv.cf, key))
		}
	}
}

// assertLen asserts the size of one of the column families.
func (t *testBuilder) assertLen(cf string, size int) {
	assert.Equal(t.t, size, t.mem.Len(cf))
}

// assertLens asserts the size of each column family.
func (t *testBuilder) assertLens(def int, lock int, write int) {
	t.assertLen(engine_util.CfDefault, def)
	t.assertLen(engine_util.CfLock, lock)
	t.assertLen(engine_util.CfWrite, write)
}

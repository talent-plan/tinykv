package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/coocood/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/tikv"
	"github.com/pingcap-incubator/tinykv/kv/tikv/inner_server"
	"github.com/pingcap-incubator/tinykv/kv/tikv/storage/exec"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

const (
	testPath = "standalone_raw_test"
)

func NewTestTiKVServer(innerServer tikv.InnerServer) *tikv.Server {
	sched := exec.NewSeqScheduler(innerServer)
	server := tikv.NewServer(innerServer, sched)
	return server
}

func newTestConfig() *config.Config {
	conf := config.DefaultConf
	conf.Engine.DBPath = filepath.Join(conf.Engine.DBPath, testPath)
	return &conf
}

func cleanUpTestData(conf *config.Config) error {
	if conf != nil {
		return os.RemoveAll(conf.Engine.DBPath)
	}
	return nil
}

func TestRawGet(t *testing.T) {
	conf := newTestConfig()
	is := inner_server.NewStandAloneInnerServer(conf)
	server := NewTestTiKVServer(is)
	defer server.Stop()
	defer cleanUpTestData(conf)

	cf := "TestRawGet"
	is.Set(nil, cf, []byte{99}, []byte{42})

	req := &kvrpcpb.RawGetRequest{
		Key: []byte{99},
		Cf:  cf,
	}
	resp, err := server.RawGet(nil, req)
	assert.Nil(t, err)
	assert.Equal(t, []byte{42}, resp.Value)
}

func TestRawPut(t *testing.T) {
	conf := newTestConfig()
	is := inner_server.NewStandAloneInnerServer(conf)
	server := NewTestTiKVServer(is)
	defer server.Stop()
	defer cleanUpTestData(conf)

	cf := "TestRawPut"
	req := &kvrpcpb.RawPutRequest{
		Key:   []byte{99},
		Value: []byte{42},
		Cf:    cf,
	}

	_, err := server.RawPut(nil, req)

	got, err := is.Get(nil, cf, []byte{99})
	assert.Nil(t, err)
	assert.Equal(t, []byte{42}, got)
}

func TestRawGetAfterRawPut(t *testing.T) {
	conf := newTestConfig()
	is := inner_server.NewStandAloneInnerServer(conf)
	server := NewTestTiKVServer(is)
	defer cleanUpTestData(conf)
	defer server.Stop()

	cf := "TestRawGetAfterRawPut"
	put := &kvrpcpb.RawPutRequest{
		Key:   []byte{99},
		Value: []byte{42},
		Cf:    cf,
	}
	_, err := server.RawPut(nil, put)
	assert.Nil(t, err)

	get := &kvrpcpb.RawGetRequest{
		Key: []byte{99},
		Cf:  cf,
	}

	resp, err := server.RawGet(nil, get)
	assert.Nil(t, err)
	assert.Equal(t, []byte{42}, resp.Value)
}

func TestRawGetAfterRawDelete(t *testing.T) {
	conf := newTestConfig()
	is := inner_server.NewStandAloneInnerServer(conf)
	server := NewTestTiKVServer(is)
	defer cleanUpTestData(conf)
	defer server.Stop()

	cf := "TestRawGetAfterRawDelete"
	assert.Nil(t, is.Set(nil, cf, []byte{99}, []byte{42}))

	delete := &kvrpcpb.RawDeleteRequest{
		Key: []byte{99},
		Cf:  cf,
	}
	get := &kvrpcpb.RawGetRequest{
		Key: []byte{99},
		Cf:  cf,
	}

	_, err := server.RawDelete(nil, delete)
	assert.Nil(t, err)

	resp, err := server.RawGet(nil, get)
	assert.Nil(t, err)
	assert.True(t, resp.NotFound)
}

func TestRawDelete(t *testing.T) {
	conf := newTestConfig()
	is := inner_server.NewStandAloneInnerServer(conf)
	server := NewTestTiKVServer(is)
	defer server.Stop()
	defer cleanUpTestData(conf)

	cf := "TestRawDelete"

	req := &kvrpcpb.RawDeleteRequest{
		Key: []byte{99},
		Cf:  cf,
	}

	_, err := server.RawDelete(nil, req)
	assert.Nil(t, err)

	_, err = is.Get(nil, cf, []byte{99})
	assert.Equal(t, err, badger.ErrKeyNotFound)
}

func TestRawScan(t *testing.T) {
	conf := newTestConfig()
	is := inner_server.NewStandAloneInnerServer(conf)
	server := NewTestTiKVServer(is)
	defer server.Stop()
	defer cleanUpTestData(conf)

	cf := "TestRawScan"

	is.Set(nil, cf, []byte{1}, []byte{233, 1})
	is.Set(nil, cf, []byte{2}, []byte{233, 2})
	is.Set(nil, cf, []byte{3}, []byte{233, 3})
	is.Set(nil, cf, []byte{4}, []byte{233, 4})
	is.Set(nil, cf, []byte{5}, []byte{233, 5})

	req := &kvrpcpb.RawScanRequest{
		StartKey: []byte{1},
		Limit:    3,
		Cf:       cf,
	}

	resp, err := server.RawScan(nil, req)
	assert.Nil(t, err)

	assert.Equal(t, 3, len(resp.Kvs))
	expectedKeys := [][]byte{{1}, {2}, {3}}
	for i, kv := range resp.Kvs {
		assert.Equal(t, expectedKeys[i], kv.Key)
		assert.Equal(t, append([]byte{233}, expectedKeys[i]...), kv.Value)
	}
}

func TestRawScanAfterRawPut(t *testing.T) {
	conf := newTestConfig()
	is := inner_server.NewStandAloneInnerServer(conf)
	server := NewTestTiKVServer(is)
	defer cleanUpTestData(conf)
	defer server.Stop()

	cf := "TestRawScanAfterRawPut"
	assert.Nil(t, is.Set(nil, cf, []byte{1}, []byte{233, 1}))
	assert.Nil(t, is.Set(nil, cf, []byte{2}, []byte{233, 2}))
	assert.Nil(t, is.Set(nil, cf, []byte{3}, []byte{233, 3}))
	assert.Nil(t, is.Set(nil, cf, []byte{4}, []byte{233, 4}))

	put := &kvrpcpb.RawPutRequest{
		Key:   []byte{5},
		Value: []byte{233, 5},
		Cf:    cf,
	}

	scan := &kvrpcpb.RawScanRequest{
		StartKey: []byte{1},
		Limit:    10,
		Cf:       cf,
	}

	expectedKeys := [][]byte{{1}, {2}, {3}, {4}, {5}}

	_, err := server.RawPut(nil, put)
	assert.Nil(t, err)

	resp, err := server.RawScan(nil, scan)
	assert.Nil(t, err)
	assert.Equal(t, len(resp.Kvs), len(expectedKeys))
	for i, kv := range resp.Kvs {
		assert.Equal(t, expectedKeys[i], kv.Key)
		assert.Equal(t, append([]byte{233}, expectedKeys[i]...), kv.Value)
	}
}

func TestRawScanAfterRawDelete(t *testing.T) {
	conf := newTestConfig()
	is := inner_server.NewStandAloneInnerServer(conf)
	server := NewTestTiKVServer(is)
	defer cleanUpTestData(conf)
	defer server.Stop()

	cf := "TestRawScanAfterRawDelete"
	assert.Nil(t, is.Set(nil, cf, []byte{1}, []byte{233, 1}))
	assert.Nil(t, is.Set(nil, cf, []byte{2}, []byte{233, 2}))
	assert.Nil(t, is.Set(nil, cf, []byte{3}, []byte{233, 3}))
	assert.Nil(t, is.Set(nil, cf, []byte{4}, []byte{233, 4}))

	delete := &kvrpcpb.RawDeleteRequest{
		Key: []byte{3},
		Cf:  cf,
	}

	scan := &kvrpcpb.RawScanRequest{
		StartKey: []byte{1},
		Limit:    10,
		Cf:       cf,
	}

	expectedKeys := [][]byte{{1}, {2}, {4}}

	_, err := server.RawDelete(nil, delete)
	assert.Nil(t, err)

	resp, err := server.RawScan(nil, scan)
	assert.Nil(t, err)
	assert.Equal(t, len(resp.Kvs), len(expectedKeys))
	for i, kv := range resp.Kvs {
		assert.Equal(t, expectedKeys[i], kv.Key)
		assert.Equal(t, append([]byte{233}, expectedKeys[i]...), kv.Value)
	}
}

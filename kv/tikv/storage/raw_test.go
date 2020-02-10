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
	testPath = "raw_test"
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

func TestGet(t *testing.T) {
	conf := newTestConfig()
	is := inner_server.NewStandAloneInnerServer(conf)
	defer cleanUpTestData(conf)

	cf := "TestRawGet"
	is.Set(nil, cf, []byte{99}, []byte{42})

	req := &kvrpcpb.RawGetRequest{
		Key: []byte{99},
		Cf:  cf,
	}

	server := NewTestTiKVServer(is)
	defer server.Stop()

	resp, err := server.RawGet(nil, req)
	assert.Nil(t, err)
	assert.Equal(t, []byte{42}, resp.Value)
}

func TestPut(t *testing.T) {
	conf := newTestConfig()
	is := inner_server.NewStandAloneInnerServer(conf)
	defer cleanUpTestData(conf)

	cf := "TestRawPut"

	req := &kvrpcpb.RawPutRequest{
		Key:   []byte{99},
		Value: []byte{42},
		Cf:    cf,
	}

	server := NewTestTiKVServer(is)
	defer server.Stop()

	_, err := server.RawPut(nil, req)

	got, err := is.Get(nil, cf, []byte{99})
	assert.Nil(t, err)
	assert.Equal(t, []byte{42}, got)
}

func TestDelete(t *testing.T) {
	conf := newTestConfig()
	is := inner_server.NewStandAloneInnerServer(conf)
	defer cleanUpTestData(conf)

	cf := "TestRawDelete"

	req := &kvrpcpb.RawDeleteRequest{
		Key: []byte{99},
		Cf:  cf,
	}

	server := NewTestTiKVServer(is)
	defer server.Stop()

	_, err := server.RawDelete(nil, req)
	assert.Nil(t, err)

	_, err = is.Get(nil, cf, []byte{99})
	assert.Equal(t, err, badger.ErrKeyNotFound)
}

func TestScan(t *testing.T) {
	conf := newTestConfig()
	is := inner_server.NewStandAloneInnerServer(conf)
	defer cleanUpTestData(conf)

	cf := "TestRawScan"

	is.Set(nil, cf, []byte{99}, []byte{42})
	is.Set(nil, cf, []byte{101}, []byte{42, 2})
	is.Set(nil, cf, []byte{102}, []byte{42, 3})
	is.Set(nil, cf, []byte{105}, []byte{42, 4})
	is.Set(nil, cf, []byte{255}, []byte{42, 5})

	req := &kvrpcpb.RawScanRequest{
		StartKey: []byte{101},
		Limit:    3,
		Cf:       cf,
	}

	server := NewTestTiKVServer(is)
	defer server.Stop()

	resp, err := server.RawScan(nil, req)
	assert.Nil(t, err)

	assert.Equal(t, 3, len(resp.Kvs))
	expectedKeys := [][]byte{{101}, {102}, {105}}
	for i, kv := range resp.Kvs {
		assert.Equal(t, expectedKeys[i], kv.Key)
		assert.Equal(t, []byte{42, byte(i + 2)}, kv.Value)
	}
}

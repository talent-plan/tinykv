package server

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/inner_server"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

const (
	testPath = "standalone_raw_test"
)

func Set(is *inner_server.StandAloneInnerServer, cf string, key []byte, value []byte) error {
	return is.Write(nil, []inner_server.Modify{
		{
			Type: inner_server.ModifyTypePut,
			Data: inner_server.Put{
				Cf:    cf,
				Key:   key,
				Value: value,
			},
		},
	})
}

func get(is *inner_server.StandAloneInnerServer, cf string, key []byte) ([]byte, error) {
	reader, err := is.Reader(nil)
	if err != nil {
		return nil, err
	}
	return reader.GetCF(cf, key)
}

func NewTestTiKVServer(innerServer inner_server.InnerServer) *Server {
	server := NewServer(innerServer)
	return server
}

func newTestConfig() *config.Config {
	conf := config.NewDefaultConfig()
	conf.DBPath = filepath.Join(conf.DBPath, testPath)
	return conf
}

func cleanUpTestData(conf *config.Config) error {
	if conf != nil {
		return os.RemoveAll(conf.DBPath)
	}
	return nil
}

func TestRawGetLab1(t *testing.T) {
	conf := newTestConfig()
	is := inner_server.NewStandAloneInnerServer(conf)
	server := NewTestTiKVServer(is)
	defer cleanUpTestData(conf)

	cf := "TestRawGet"
	Set(is, cf, []byte{99}, []byte{42})

	req := &kvrpcpb.RawGetRequest{
		Key: []byte{99},
		Cf:  cf,
	}
	resp, err := server.RawGet(nil, req)
	assert.Nil(t, err)
	assert.Equal(t, []byte{42}, resp.Value)
}

func TestRawGetNotFoundLab1(t *testing.T) {
	conf := newTestConfig()
	is := inner_server.NewStandAloneInnerServer(conf)
	server := NewTestTiKVServer(is)
	defer cleanUpTestData(conf)

	cf := "TestRawGetNotFound"
	req := &kvrpcpb.RawGetRequest{
		Key: []byte{99},
		Cf:  cf,
	}
	resp, err := server.RawGet(nil, req)
	assert.Nil(t, err)
	assert.True(t, resp.NotFound)
}

func TestRawPutLab1(t *testing.T) {
	conf := newTestConfig()
	is := inner_server.NewStandAloneInnerServer(conf)
	server := NewTestTiKVServer(is)
	defer cleanUpTestData(conf)

	cf := "TestRawPut"
	req := &kvrpcpb.RawPutRequest{
		Key:   []byte{99},
		Value: []byte{42},
		Cf:    cf,
	}

	_, err := server.RawPut(nil, req)

	got, err := get(is, cf, []byte{99})
	assert.Nil(t, err)
	assert.Equal(t, []byte{42}, got)
}

func TestRawGetAfterRawPutLab1(t *testing.T) {
	conf := newTestConfig()
	is := inner_server.NewStandAloneInnerServer(conf)
	server := NewTestTiKVServer(is)
	defer cleanUpTestData(conf)

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

func TestRawGetAfterRawDeleteLab1(t *testing.T) {
	conf := newTestConfig()
	is := inner_server.NewStandAloneInnerServer(conf)
	server := NewTestTiKVServer(is)
	defer cleanUpTestData(conf)

	cf := "TestRawGetAfterRawDelete"
	assert.Nil(t, Set(is, cf, []byte{99}, []byte{42}))

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

func TestRawDeleteLab1(t *testing.T) {
	conf := newTestConfig()
	is := inner_server.NewStandAloneInnerServer(conf)
	server := NewTestTiKVServer(is)
	defer cleanUpTestData(conf)

	cf := "TestRawDelete"

	req := &kvrpcpb.RawDeleteRequest{
		Key: []byte{99},
		Cf:  cf,
	}

	_, err := server.RawDelete(nil, req)
	assert.Nil(t, err)

	_, err = get(is, cf, []byte{99})
	assert.Equal(t, err, badger.ErrKeyNotFound)
}

func TestRawScanLab1(t *testing.T) {
	conf := newTestConfig()
	is := inner_server.NewStandAloneInnerServer(conf)
	server := NewTestTiKVServer(is)
	defer cleanUpTestData(conf)

	cf := "TestRawScan"

	Set(is, cf, []byte{1}, []byte{233, 1})
	Set(is, cf, []byte{2}, []byte{233, 2})
	Set(is, cf, []byte{3}, []byte{233, 3})
	Set(is, cf, []byte{4}, []byte{233, 4})
	Set(is, cf, []byte{5}, []byte{233, 5})

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

func TestRawScanAfterRawPutLab1(t *testing.T) {
	conf := newTestConfig()
	is := inner_server.NewStandAloneInnerServer(conf)
	server := NewTestTiKVServer(is)
	defer cleanUpTestData(conf)

	cf := "TestRawScanAfterRawPut"
	assert.Nil(t, Set(is, cf, []byte{1}, []byte{233, 1}))
	assert.Nil(t, Set(is, cf, []byte{2}, []byte{233, 2}))
	assert.Nil(t, Set(is, cf, []byte{3}, []byte{233, 3}))
	assert.Nil(t, Set(is, cf, []byte{4}, []byte{233, 4}))

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

func TestRawScanAfterRawDeleteLab1(t *testing.T) {
	conf := newTestConfig()
	is := inner_server.NewStandAloneInnerServer(conf)
	server := NewTestTiKVServer(is)
	defer cleanUpTestData(conf)

	cf := "TestRawScanAfterRawDelete"
	assert.Nil(t, Set(is, cf, []byte{1}, []byte{233, 1}))
	assert.Nil(t, Set(is, cf, []byte{2}, []byte{233, 2}))
	assert.Nil(t, Set(is, cf, []byte{3}, []byte{233, 3}))
	assert.Nil(t, Set(is, cf, []byte{4}, []byte{233, 4}))

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

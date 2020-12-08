package standalone_storage

import (
	"os"
	"testing"

	"github.com/alecthomas/assert"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

func cleanUpTestData(conf *config.Config) error {
	if conf != nil {
		return os.RemoveAll(conf.DBPath)
	}
	return nil
}

func TestReader(t *testing.T) {
	conf := config.NewTestConfig()
	s := NewStandAloneStorage(conf)
	s.Start()

	defer cleanUpTestData(conf)
	defer s.Stop()

	cf := engine_util.CfDefault
	ctx := &kvrpcpb.Context{}
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Key:   []byte("a"),
				Value: []byte("x"),
				Cf:    cf,
			},
		},
	}
	s.Write(ctx, batch)

	r, _ := s.Reader(ctx)
	ret, _ := r.GetCF(cf, []byte("a"))

	assert.Equal(t, []byte("x"), ret)
}

func TestIterCF(t *testing.T) {
	conf := config.NewTestConfig()
	s := NewStandAloneStorage(conf)
	s.Start()

	defer cleanUpTestData(conf)
	defer s.Stop()

	cf := engine_util.CfDefault
	ctx := &kvrpcpb.Context{}
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Key:   []byte("a"),
				Value: []byte("x"),
				Cf:    cf,
			},
		},
		{
			Data: storage.Put{
				Key:   []byte("b"),
				Value: []byte("y"),
				Cf:    cf,
			},
		},
	}
	s.Write(ctx, batch)

	r, _ := s.Reader(ctx)
	iter := r.IterCF(cf)
	iter.Seek([]byte("a"))
	item := iter.Item()
	assert.Equal(t, []byte("a"), item.Key())
	val, _ := item.Value()
	assert.Equal(t, []byte("x"), val)

	iter.Next()
	item = iter.Item()
	assert.Equal(t, []byte("b"), item.Key())
	val, _ = item.Value()
	assert.Equal(t, []byte("y"), val)
	iter.Close()
}

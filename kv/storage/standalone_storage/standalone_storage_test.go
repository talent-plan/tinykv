package standalone_storage

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

func TestNewStandAloneStorage(t *testing.T) {
	// type assert
	var _ storage.Storage = new(StandAloneStorage)
	conf := config.NewDefaultConfig()
	standalone := NewStandAloneStorage(conf)
	require.NotNil(t, standalone)

	reader, err := standalone.Reader(&kvrpcpb.Context{})
	require.NoError(t, err)
	require.NotNil(t, reader)
}

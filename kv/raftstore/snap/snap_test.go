package snap

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync/atomic"
	"testing"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	snapTestKey        = []byte("tkey")
	regionTestBegin    = []byte("ta")
	regionTestBeginOld = []byte("ua")
	regionTestEnd      = []byte("tz")
	regionTestEndOld   = []byte("uz")
)

const (
	testWriteBatchSize = 10 * 1024 * 1024
)

type dummyDeleter struct{}

func (d *dummyDeleter) DeleteSnapshot(key SnapKey, snapshot Snapshot, checkEntry bool) bool {
	snapshot.Delete()
	return true
}

func openDB(t *testing.T, dir string) *badger.DB {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)
	require.Nil(t, err)
	return db
}

func fillDBData(t *testing.T, db *badger.DB) {
	// write some data for multiple cfs.
	wb := new(engine_util.WriteBatch)
	value := make([]byte, 32)
	wb.SetCF(engine_util.CfDefault, snapTestKey, value)
	wb.SetCF(engine_util.CfWrite, snapTestKey, value)
	wb.SetCF(engine_util.CfLock, snapTestKey, value)
	err := wb.WriteToDB(db)
	require.Nil(t, err)
}

func getKVCount(t *testing.T, db *badger.DB) int {
	count := 0
	err := db.View(func(txn *badger.Txn) error {
		for _, cf := range engine_util.CFs {
			it := engine_util.NewCFIterator(cf, txn)
			defer it.Close()
			for it.Seek(regionTestBegin); it.Valid(); it.Next() {
				if bytes.Compare(it.Item().Key(), regionTestEnd) >= 0 {
					break
				}
				count++
			}
		}
		return nil
	})

	assert.Nil(t, err)
	return count
}

func genTestRegion(regionID, storeID, peerID uint64) *metapb.Region {
	return &metapb.Region{
		Id:       regionID,
		StartKey: regionTestBegin,
		EndKey:   regionTestEnd,
		RegionEpoch: &metapb.RegionEpoch{
			Version: 1,
			ConfVer: 1,
		},
		Peers: []*metapb.Peer{
			{StoreId: storeID, Id: peerID},
		},
	}
}

func assertEqDB(t *testing.T, expected, actual *badger.DB) {
	for _, cf := range engine_util.CFs {
		expectedVal := getDBValue(t, expected, cf, snapTestKey)
		actualVal := getDBValue(t, actual, cf, snapTestKey)
		assert.Equal(t, expectedVal, actualVal)
	}
}

func getDBValue(t *testing.T, db *badger.DB, cf string, key []byte) (val []byte) {
	val, err := engine_util.GetCF(db, cf, key)
	require.Nil(t, err, string(key))
	return val
}

func TestSnapGenMeta(t *testing.T) {
	cfFiles := make([]*CFFile, 0, len(engine_util.CFs))
	for i, cf := range engine_util.CFs {
		f := &CFFile{
			CF:       cf,
			Size:     100 * uint64(i+1),
			Checksum: 1000 * uint32(i+1),
		}
		cfFiles = append(cfFiles, f)
	}
	meta, err := genSnapshotMeta(cfFiles)
	require.Nil(t, err)
	for i, cfFileMeta := range meta.CfFiles {
		assert.Equal(t, cfFiles[i].CF, cfFileMeta.Cf)
		assert.Equal(t, cfFiles[i].Size, cfFileMeta.Size_)
		assert.Equal(t, cfFiles[i].Checksum, cfFileMeta.Checksum)
	}
}

func TestSnapDisplayPath(t *testing.T) {
	dir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	key := &SnapKey{1, 1, 2}
	prefix := fmt.Sprintf("%s_%s", snapGenPrefix, key)
	displayPath := getDisplayPath(dir, prefix)
	assert.NotEqual(t, displayPath, "")
}

func TestSnapFile(t *testing.T) {
	doTestSnapFile(t, true)
	doTestSnapFile(t, false)
}

func doTestSnapFile(t *testing.T, dbHasData bool) {
	regionID := uint64(1)
	region := genTestRegion(regionID, 1, 1)
	dir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	db := openDB(t, dir)
	if dbHasData {
		fillDBData(t, db)
	}

	snapDir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(snapDir)
	key := SnapKey{RegionID: regionID, Term: 1, Index: 1}
	sizeTrack := new(int64)
	deleter := &dummyDeleter{}
	s1, err := NewSnapForBuilding(snapDir, key, sizeTrack, deleter)
	require.Nil(t, err)
	// Ensure that this snapshot file doesn't exist before being built.
	assert.False(t, s1.Exists())
	assert.Equal(t, int64(0), atomic.LoadInt64(sizeTrack))

	snapData := new(rspb.RaftSnapshotData)
	snapData.Region = region
	stat := new(SnapStatistics)
	assert.Nil(t, s1.Build(db.NewTransaction(false), region, snapData, stat, deleter))

	// Ensure that this snapshot file does exist after being built.
	assert.True(t, s1.Exists())
	totalSize := s1.TotalSize()
	// Ensure the `size_track` is modified correctly.
	size := atomic.LoadInt64(sizeTrack)
	assert.Equal(t, int64(totalSize), size)
	assert.Equal(t, int64(stat.Size), size)
	if dbHasData {
		assert.Equal(t, 3, getKVCount(t, db))
		// stat.KVCount is 5 because there are two extra default cf value.
		assert.Equal(t, 3, stat.KVCount)
	}

	// Ensure this snapshot could be read for sending.
	s2, err := NewSnapForSending(snapDir, key, sizeTrack, deleter)
	require.Nil(t, err, errors.ErrorStack(err))
	assert.True(t, s2.Exists())

	dstDir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(dstDir)

	s3, err := NewSnapForReceiving(dstDir, key, snapData.Meta, sizeTrack, deleter)
	require.Nil(t, err)
	assert.False(t, s3.Exists())

	// Ensure snapshot data could be read out of `s2`, and write into `s3`.
	copySize, err := io.Copy(s3, s2)
	require.Nil(t, err)
	assert.Equal(t, size, copySize)
	assert.False(t, s3.Exists())
	assert.Nil(t, s3.Save())
	assert.True(t, s3.Exists())

	// Ensure the tracked size is handled correctly after receiving a snapshot.
	assert.Equal(t, size*2, atomic.LoadInt64(sizeTrack))

	// Ensure `delete()` works to delete the source snapshot.
	s2.Delete()
	assert.False(t, s2.Exists())
	assert.False(t, s1.Exists())
	assert.Equal(t, size, atomic.LoadInt64(sizeTrack))

	// Ensure a snapshot could be applied to DB.
	s4, err := NewSnapForApplying(dstDir, key, sizeTrack, deleter)
	require.Nil(t, err)
	assert.True(t, s4.Exists())

	dstDBDir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(dstDBDir)

	dstDB := openDB(t, dstDBDir)
	opts := ApplyOptions{
		DB:     dstDB,
		Region: region,
	}
	err = s4.Apply(opts)
	require.Nil(t, err, errors.ErrorStack(err))

	// Ensure `delete()` works to delete the dest snapshot.
	s4.Delete()
	assert.False(t, s4.Exists())
	assert.False(t, s3.Exists())
	assert.Equal(t, int64(0), atomic.LoadInt64(sizeTrack))

	// Verify the data is correct after applying snapshot.
	if dbHasData {
		assertEqDB(t, db, dstDB)
	}
}

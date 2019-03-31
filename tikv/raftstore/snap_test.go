package raftstore

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/coocood/badger"
	"github.com/cznic/mathutil"
	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/pkg/fileutil"
)

var (
	snapTestKey        = []byte("tkey")
	snapTestKeyOld     = encodeOldKey(snapTestKey, 100)
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

func getTestDBForRegions(t *testing.T, path string, regions []uint64) *DBBundle {
	kv := openDBBundle(t, path)
	fillDBBundleData(t, kv)
	for _, regionID := range regions {
		// Put apply state into kv engine.
		applyState := applyState{
			appliedIndex:   10,
			truncatedIndex: 10,
		}
		require.Nil(t, putValue(kv.db, ApplyStateKey(regionID), applyState.Marshal()))

		// Put region ifno into kv engine.
		region := genTestRegion(regionID, 1, 1)
		regionState := new(rspb.RegionLocalState)
		regionState.Region = region
		require.Nil(t, putMsg(kv.db, RegionStateKey(regionID), regionState))
	}
	return kv
}

func getKVCount(t *testing.T, db *DBBundle) int {
	count := 0
	err := db.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(regionTestBegin); it.Valid(); it.Next() {
			if bytes.Compare(it.Item().Key(), regionTestEnd) >= 0 {
				break
			}
			count++
		}
		for it.Seek(regionTestBeginOld); it.Valid(); it.Next() {
			if bytes.Compare(it.Item().Key(), regionTestEndOld) >= 0 {
				break
			}
			count++
		}
		return nil
	})
	lockIterator := db.lockStore.NewIterator()
	for lockIterator.Seek(regionTestBegin); lockIterator.Valid(); lockIterator.Next() {
		if bytes.Compare(lockIterator.Key(), regionTestEnd) >= 0 {
			break
		}
		count++
	}
	assert.Nil(t, err)
	return count
}

func genTestRegion(regionID, storeID, peerID uint64) *metapb.Region {
	return &metapb.Region{
		Id:       regionID,
		StartKey: codec.EncodeBytes(nil, regionTestBegin),
		EndKey:   codec.EncodeBytes(nil, regionTestEnd),
		RegionEpoch: &metapb.RegionEpoch{
			Version: 1,
			ConfVer: 1,
		},
		Peers: []*metapb.Peer{
			{StoreId: storeID, Id: peerID},
		},
	}
}

func assertEqDB(t *testing.T, expected, actual *DBBundle) {
	expectedVal := getDBValue(t, expected.db, snapTestKey)
	actualVal := getDBValue(t, actual.db, snapTestKey)
	assert.Equal(t, expectedVal, actualVal)
	expectedVal = getDBValue(t, expected.db, snapTestKeyOld)
	actualVal = getDBValue(t, actual.db, snapTestKeyOld)
	assert.Equal(t, expectedVal, actualVal)
	expectedLock := expected.lockStore.Get(snapTestKey, nil)
	actualLock := actual.lockStore.Get(snapTestKey, nil)
	assert.Equal(t, expectedLock, actualLock)
}

func getDBValue(t *testing.T, db *badger.DB, key []byte) (val []byte) {
	require.Nil(t, db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		require.Nil(t, err, string(key))
		val, err = item.Value()
		require.Nil(t, err)
		return nil
	}))
	return
}

func openDBBundle(t *testing.T, dir string) *DBBundle {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)
	require.Nil(t, err)
	lockStore := lockstore.NewMemStore(1024)
	rollbackStore := lockstore.NewMemStore(1024)
	return &DBBundle{
		db:            db,
		lockStore:     lockStore,
		rollbackStore: rollbackStore,
	}
}

func fillDBBundleData(t *testing.T, dbBundle *DBBundle) {
	// write some data.
	// Put an new version key and an old version key.
	err := dbBundle.db.Update(func(txn *badger.Txn) error {
		value := make([]byte, 32)
		require.Nil(t, txn.SetWithMetaSlice(snapTestKey, value, mvcc.NewDBUserMeta(150, 200)))
		oldUseMeta := mvcc.NewDBUserMeta(50, 100).ToOldUserMeta(200)
		require.Nil(t, txn.SetWithMetaSlice(snapTestKeyOld, make([]byte, 128), oldUseMeta))
		return nil
	})
	require.Nil(t, err)
	lockVal := &mvcc.MvccLock{
		MvccLockHdr: mvcc.MvccLockHdr{
			StartTS:    250,
			TTL:        100,
			Op:         byte(kvrpcpb.Op_Put),
			PrimaryLen: uint16(len(snapTestKey)),
			HasOldVer:  true,
		},
		Primary: snapTestKey,
		Value:   make([]byte, 128),
		OldVal:  make([]byte, 32),
		OldMeta: mvcc.NewDBUserMeta(150, 200),
	}
	dbBundle.lockStore.Insert(snapTestKey, lockVal.MarshalBinary())
}

func TestSnapGenMeta(t *testing.T) {
	cfFiles := make([]*CFFile, 0, len(snapshotCFs))
	for i, cf := range snapshotCFs {
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
		assert.Equal(t, cfFileMeta.Cf, cfFiles[i].CF)
		assert.Equal(t, cfFileMeta.Size_, cfFiles[i].Size)
		assert.Equal(t, cfFileMeta.Checksum, cfFiles[i].Checksum)
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
	dbBundle := openDBBundle(t, dir)
	if dbHasData {
		fillDBBundleData(t, dbBundle)
	}

	snapDir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(snapDir)
	key := SnapKey{RegionID: regionID, Term: 1, Index: 1}
	sizeTrack := new(int64)
	deleter := &dummyDeleter{}
	s1, err := NewSnapForBuilding(snapDir, key, sizeTrack, deleter, nil)
	require.Nil(t, err)
	// Ensure that this snapshot file doesn't exist before being built.
	assert.False(t, s1.Exists())
	assert.Equal(t, int64(0), atomic.LoadInt64(sizeTrack))

	snapData := new(rspb.RaftSnapshotData)
	snapData.Region = region
	stat := new(SnapStatistics)
	assert.Nil(t, s1.Build(dbBundle, region, snapData, stat, deleter))

	// Ensure that this snapshot file does exist after being built.
	assert.True(t, s1.Exists())
	totalSize := s1.TotalSize()
	// Ensure the `size_track` is modified correctly.
	size := atomic.LoadInt64(sizeTrack)
	assert.Equal(t, int64(totalSize), size)
	assert.Equal(t, int64(stat.Size), size)
	if dbHasData {
		assert.Equal(t, 3, getKVCount(t, dbBundle))
		// stat.KVCount is 5 because there are two extra default cf value.
		assert.Equal(t, 5, stat.KVCount)
	}

	// Ensure this snapshot could be read for sending.
	s2, err := NewSnapForSending(snapDir, key, sizeTrack, deleter)
	require.Nil(t, err, errors.ErrorStack(err))
	assert.True(t, s2.Exists())

	dstDir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(dstDir)

	s3, err := NewSnapForReceiving(dstDir, key, snapData.Meta, sizeTrack, deleter, nil)
	require.Nil(t, err)
	assert.False(t, s3.Exists())

	// Ensure snapshot data could be read out of `s2`, and write into `s3`.
	copySize, err := io.Copy(s3, s2)
	require.Nil(t, err)
	assert.Equal(t, copySize, size)
	assert.False(t, s3.Exists())
	assert.Nil(t, s3.Save())
	assert.True(t, s3.Exists())

	// Ensure the tracked size is handled correctly after receiving a snapshot.
	assert.Equal(t, atomic.LoadInt64(sizeTrack), size*2)

	// Ensure `delete()` works to delete the source snapshot.
	s2.Delete()
	assert.False(t, s2.Exists())
	assert.False(t, s1.Exists())
	assert.Equal(t, atomic.LoadInt64(sizeTrack), size)

	// Ensure a snapshot could be applied to DB.
	s4, err := NewSnapForApplying(dstDir, key, sizeTrack, deleter)
	require.Nil(t, err)
	assert.True(t, s4.Exists())

	dstDBDir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(dstDBDir)

	dstDBBundle := openDBBundle(t, dstDBDir)
	abort := new(uint64)
	*abort = JobStatusRunning
	opts := ApplyOptions{
		DBBundle:  dstDBBundle,
		Region:    region,
		Abort:     abort,
		BatchSize: testWriteBatchSize,
	}
	err = s4.Apply(opts)
	require.Nil(t, err, errors.ErrorStack(err))

	// Ensure `delete()` works to delete the dest snapshot.
	s4.Delete()
	assert.False(t, s4.Exists())
	assert.False(t, s3.Exists())
	assert.Equal(t, atomic.LoadInt64(sizeTrack), int64(0))

	// Verify the data is correct after applying snapshot.
	if dbHasData {
		assertEqDB(t, dbBundle, dstDBBundle)
	}
}

func TestSnapValidation(t *testing.T) {
	doTestSnapValidation(t, false)
	doTestSnapValidation(t, true)
}

func doTestSnapValidation(t *testing.T, dbHasData bool) {
	regionID := uint64(1)
	region := genTestRegion(regionID, 1, 1)
	dir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	dbBundle := openDBBundle(t, dir)
	if dbHasData {
		fillDBBundleData(t, dbBundle)
	}

	snapDir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(snapDir)
	key := SnapKey{RegionID: regionID, Term: 1, Index: 1}
	sizeTrack := new(int64)
	deleter := &dummyDeleter{}
	s1, err := NewSnapForBuilding(snapDir, key, sizeTrack, deleter, nil)
	require.Nil(t, err)
	assert.False(t, s1.Exists())

	snapData := new(rspb.RaftSnapshotData)
	snapData.Region = region
	stat := new(SnapStatistics)
	assert.Nil(t, s1.Build(dbBundle, region, snapData, stat, deleter))
	assert.True(t, s1.Exists())

	s2, err := NewSnapForBuilding(snapDir, key, sizeTrack, deleter, nil)
	require.Nil(t, err)
	assert.True(t, s2.Exists())
	assert.Nil(t, s2.Build(dbBundle, region, snapData, stat, deleter))
	assert.True(t, s2.Exists())
}

func TestSnapshotCorruptionSizeOrChecksum(t *testing.T) {
	regionID := uint64(1)
	region := genTestRegion(regionID, 1, 1)
	dir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	dbBundle := openDBBundle(t, dir)
	fillDBBundleData(t, dbBundle)

	snapDir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(snapDir)
	key := SnapKey{RegionID: regionID, Term: 1, Index: 1}
	sizeTrack := new(int64)
	deleter := &dummyDeleter{}
	s1, err := NewSnapForBuilding(snapDir, key, sizeTrack, deleter, nil)
	assert.False(t, s1.Exists())
	snapData := new(rspb.RaftSnapshotData)
	snapData.Region = region
	stat := new(SnapStatistics)
	assert.Nil(t, s1.Build(dbBundle, region, snapData, stat, deleter))

	corruptSnapSizeIn(t, snapDir)
	_, err = NewSnapForSending(snapDir, key, sizeTrack, deleter)
	require.NotNil(t, err)

	s2, err := NewSnapForBuilding(snapDir, key, sizeTrack, deleter, nil)
	assert.False(t, s2.Exists())
	assert.Nil(t, s2.Build(dbBundle, region, snapData, stat, deleter))
	assert.True(t, s2.Exists())

	dstDir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(dstDir)

	copySnapshotForTest(t, snapDir, dstDir, key, sizeTrack, snapData.Meta, deleter)

	metas := corruptSnapshotChecksumIn(t, dstDir)
	assert.Equal(t, 1, len(metas))

	snapMeta := metas[0]
	s5, err := NewSnapForApplying(dstDir, key, sizeTrack, deleter)
	require.Nil(t, err)
	require.True(t, s5.Exists())

	dstDBDir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(dstDBDir)

	abort := new(uint64)
	*abort = JobStatusRunning
	dstDBBundle := openDBBundle(t, dstDBDir)
	opts := ApplyOptions{
		DBBundle:  dstDBBundle,
		Region:    region,
		Abort:     abort,
		BatchSize: testWriteBatchSize,
	}
	require.NotNil(t, s5.Apply(opts))

	corruptSnapSizeIn(t, dstDir)
	_, err = NewSnapForReceiving(dstDir, key, snapMeta, sizeTrack, deleter, nil)
	require.NotNil(t, err)
	_, err = NewSnapForApplying(dstDir, key, sizeTrack, deleter)
	require.NotNil(t, err)
}

func corruptSnapSizeIn(t *testing.T, dir string) {
	filePaths, err := fileutil.ReadDir(dir)
	require.Nil(t, err)
	for _, filePath := range filePaths {
		if !strings.HasSuffix(filePath, metaFileSuffix) {
			file, err1 := os.OpenFile(filepath.Join(dir, filePath), os.O_APPEND|os.O_WRONLY, 0600)
			require.Nil(t, err1)
			_, err1 = file.Write([]byte("xxxxx"))
			require.Nil(t, err1)
		}
	}
}

func copySnapshotForTest(t *testing.T, fromDir, toDir string, key SnapKey, sizeTrack *int64,
	snapMeta *rspb.SnapshotMeta, deleter SnapshotDeleter) {
	fromSnap, err := NewSnapForSending(fromDir, key, sizeTrack, deleter)
	require.Nil(t, err)
	assert.True(t, fromSnap.Exists())
	toSnap, err := NewSnapForReceiving(toDir, key, snapMeta, sizeTrack, deleter, nil)
	assert.False(t, toSnap.Exists())
	_, err = io.Copy(toSnap, fromSnap)
	assert.Nil(t, err)
	assert.Nil(t, toSnap.Save())
	assert.True(t, toSnap.Exists())
}

func corruptSnapshotChecksumIn(t *testing.T, dir string) (result []*rspb.SnapshotMeta) {
	filePaths, err1 := fileutil.ReadDir(dir)
	require.Nil(t, err1)
	for _, fileName := range filePaths {
		if strings.HasSuffix(fileName, metaFileSuffix) {
			fileFullPath := filepath.Join(dir, fileName)
			snapMeta := new(rspb.SnapshotMeta)
			data, err := ioutil.ReadFile(fileFullPath)
			require.Nil(t, err)
			require.Nil(t, snapMeta.Unmarshal(data))
			for _, cf := range snapMeta.CfFiles {
				cf.Checksum += 100
			}
			data, err = snapMeta.Marshal()
			require.Nil(t, err)
			file, err := os.OpenFile(fileFullPath, os.O_TRUNC|os.O_WRONLY, 0600)
			require.Nil(t, err)
			_, err = file.Write(data)
			require.Nil(t, err)
			file.Close()
			result = append(result, snapMeta)
		}
	}
	return
}

func corruptSnapshotMetaFile(t *testing.T, dir string) int {
	total := 0
	filePaths, err1 := fileutil.ReadDir(dir)
	require.Nil(t, err1)
	for _, fileName := range filePaths {
		if strings.HasSuffix(fileName, metaFileSuffix) {
			fileFullPath := filepath.Join(dir, fileName)
			data, err := ioutil.ReadFile(fileFullPath)
			require.Nil(t, err)
			// Make the last byte of the meta file corrupted
			// by turning over all bits of it
			lastByte := &data[len(data)-1]
			*lastByte = ^(*lastByte)
			file, err := os.OpenFile(fileFullPath, os.O_TRUNC|os.O_WRONLY, 0600)
			require.Nil(t, err)
			_, err = file.Write(data)
			require.Nil(t, err)
			file.Close()
			total++
		}
	}
	return total
}

func TestSnapshotCorruptionOnMetaFile(t *testing.T) {
	regionID := uint64(1)
	region := genTestRegion(regionID, 1, 1)
	dir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	dbBundle := openDBBundle(t, dir)
	fillDBBundleData(t, dbBundle)

	snapDir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(snapDir)
	key := SnapKey{RegionID: regionID, Term: 1, Index: 1}
	sizeTrack := new(int64)
	deleter := &dummyDeleter{}
	s1, err := NewSnapForBuilding(snapDir, key, sizeTrack, deleter, nil)
	assert.False(t, s1.Exists())
	snapData := new(rspb.RaftSnapshotData)
	snapData.Region = region
	stat := new(SnapStatistics)
	assert.Nil(t, s1.Build(dbBundle, region, snapData, stat, deleter))

	assert.Equal(t, 1, corruptSnapshotMetaFile(t, snapDir))
	_, err = NewSnapForSending(snapDir, key, sizeTrack, deleter)
	require.NotNil(t, err)

	s2, err := NewSnapForBuilding(snapDir, key, sizeTrack, deleter, nil)
	assert.False(t, s2.Exists())
	assert.Nil(t, s2.Build(dbBundle, region, snapData, stat, deleter))
	assert.True(t, s2.Exists())

	dstDir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(dstDir)

	copySnapshotForTest(t, snapDir, dstDir, key, sizeTrack, snapData.Meta, deleter)

	assert.Equal(t, 1, corruptSnapshotMetaFile(t, dstDir))

	_, err = NewSnapForApplying(dstDir, key, sizeTrack, deleter)
	require.NotNil(t, err)
	_, err = NewSnapForReceiving(dstDir, key, snapData.Meta, sizeTrack, deleter, nil)
	require.NotNil(t, err)
}

func TestSnapMgrCreateDir(t *testing.T) {
	// Ensure `mgr` creates the specified directory when it does not exist.
	tempDir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(tempDir)
	tempPath := filepath.Join(tempDir, "snap1")
	_, err = os.Stat(tempPath)
	require.True(t, os.IsNotExist(err))
	mgr := NewSnapManager(tempPath, nil)
	require.Nil(t, mgr.init())
	_, err = os.Stat(tempPath)
	require.Nil(t, err)

	// Ensure `init()` will return an error if specified target is a file.
	tempPath2 := filepath.Join(tempDir, "snap2")
	f2, err := os.Create(tempPath2)
	require.Nil(t, err)
	f2.Close()
	mgr = NewSnapManager(tempPath2, nil)
	require.NotNil(t, mgr.init())
}

func TestSnapMgrV2(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(tempDir)
	mgr := NewSnapManager(tempDir, nil)
	require.Nil(t, mgr.init())
	require.Equal(t, uint64(0), mgr.GetTotalSnapSize())

	dbDir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(dbDir)

	dbBundle := openDBBundle(t, dbDir)
	fillDBBundleData(t, dbBundle)

	key1 := SnapKey{RegionID: 1, Term: 1, Index: 1}
	sizeTrack := new(int64)
	deleter := &dummyDeleter{}
	s1, err := NewSnapForBuilding(tempDir, key1, sizeTrack, deleter, nil)
	require.Nil(t, err)
	region := genTestRegion(1, 1, 1)
	snapData := new(rspb.RaftSnapshotData)
	snapData.Region = region
	stat := new(SnapStatistics)
	require.Nil(t, s1.Build(dbBundle, region, snapData, stat, deleter))

	s, err := NewSnapForSending(tempDir, key1, sizeTrack, deleter)
	require.Nil(t, err)
	expectedSize := s.TotalSize()
	s2, err := NewSnapForReceiving(tempDir, key1, snapData.Meta, sizeTrack, deleter, nil)
	require.Nil(t, err)
	n, err := io.Copy(s2, s)
	require.Nil(t, err)
	require.Equal(t, n, int64(expectedSize))
	require.Nil(t, s2.Save())

	key2 := SnapKey{RegionID: 2, Term: 1, Index: 1}
	region.Id = 2
	snapData.Region = region

	s3, err := NewSnapForBuilding(tempDir, key2, sizeTrack, deleter, nil)
	require.Nil(t, err)
	s4, err := NewSnapForReceiving(tempDir, key2, snapData.Meta, sizeTrack, deleter, nil)
	require.Nil(t, err)

	require.True(t, s1.Exists())
	require.True(t, s2.Exists())
	require.False(t, s3.Exists())
	require.False(t, s4.Exists())

	mgr = NewSnapManager(tempDir, nil)
	err = mgr.init()
	require.Nil(t, err, errors.ErrorStack(err))
	require.Equal(t, expectedSize*2, mgr.GetTotalSnapSize())

	require.True(t, s1.Exists())
	require.True(t, s2.Exists())
	require.False(t, s3.Exists())
	require.False(t, s4.Exists())

	snap, err := mgr.GetSnapshotForSending(key1)
	require.Nil(t, err)
	snap.Delete()
	assert.Equal(t, expectedSize, mgr.GetTotalSnapSize())
	snap, err = mgr.GetSnapshotForApplying(key1)
	require.Nil(t, err)
	snap.Delete()
	assert.Equal(t, uint64(0), mgr.GetTotalSnapSize())
}

func checkRegistryAroundDeregister(t *testing.T, mgr *SnapManager, key SnapKey, entry SnapEntry) {
	snapKeys, err := mgr.ListIdleSnap()
	require.Nil(t, err)
	require.Len(t, snapKeys, 0)
	require.True(t, mgr.HasRegistered(key))
	mgr.Deregister(key, entry)
	snapKeys, err = mgr.ListIdleSnap()
	assert.Len(t, snapKeys, 1)
	assert.Equal(t, key, snapKeys[0].SnapKey)
	assert.False(t, mgr.HasRegistered(key))
}

func TestSnapDeletionOnRegistry(t *testing.T) {
	srcTmpDir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(srcTmpDir)
	srcMgr := NewSnapManager(srcTmpDir, nil)
	require.Nil(t, srcMgr.init())

	srcDBDir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(srcDBDir)
	dbBundle := openDBBundle(t, srcDBDir)
	fillDBBundleData(t, dbBundle)

	key := SnapKey{RegionID: 1, Term: 1, Index: 1}
	region := genTestRegion(1, 1, 1)

	// Ensure the snapshot being built will not be deleted on GC.
	srcMgr.Register(key, SnapEntryGenerating)
	s1, err := srcMgr.GetSnapshotForBuilding(key, dbBundle)
	require.Nil(t, err)
	snapData := new(rspb.RaftSnapshotData)
	snapData.Region = region
	stat := new(SnapStatistics)
	require.Nil(t, s1.Build(dbBundle, region, snapData, stat, srcMgr))

	snapBin, err := snapData.Marshal()
	require.Nil(t, err)

	checkRegistryAroundDeregister(t, srcMgr, key, SnapEntryGenerating)

	// Ensure the snapshot being sent will not be deleted on GC.
	srcMgr.Register(key, SnapEntrySending)
	s2, err := srcMgr.GetSnapshotForSending(key)
	require.Nil(t, err)
	expectedSize := s2.TotalSize()

	dstTmpDir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(dstTmpDir)
	dstMgr := NewSnapManager(dstTmpDir, nil)
	require.Nil(t, dstMgr.init())

	// Ensure the snapshot being received will not be deleted on GC.
	dstMgr.Register(key, SnapEntryReceiving)
	s3, err := dstMgr.GetSnapshotForReceiving(key, snapBin)
	require.Nil(t, err)
	n, err := io.Copy(s3, s2)
	require.Nil(t, err)
	assert.Equal(t, expectedSize, uint64(n))
	require.Nil(t, s3.Save())

	checkRegistryAroundDeregister(t, srcMgr, key, SnapEntrySending)
	checkRegistryAroundDeregister(t, dstMgr, key, SnapEntryReceiving)

	// Ensure the snapshot to be applied will not be deleted on GC.
	snapKeys, err := dstMgr.ListIdleSnap()
	require.Nil(t, err)
	require.Len(t, snapKeys, 1)
	snapKey := snapKeys[0].SnapKey
	require.Equal(t, key, snapKey)
	assert.False(t, dstMgr.HasRegistered(snapKey))
	dstMgr.Register(snapKey, SnapEntryApplying)
	s4, err := dstMgr.GetSnapshotForApplying(snapKey)
	require.Nil(t, err)
	s5, err := dstMgr.GetSnapshotForApplying(snapKey)
	require.Nil(t, err)
	dstMgr.DeleteSnapshot(snapKey, s4, false)
	assert.True(t, s5.Exists())
}

func TestSnapMaxTotalSize(t *testing.T) {
	regions := make([]uint64, 20)
	for i := 0; i < 20; i++ {
		regions[i] = uint64(i)
	}
	kvPath, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(kvPath)
	dbBundle := getTestDBForRegions(t, kvPath, regions)

	snapFilePath, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(snapFilePath)

	maxTotalSize := uint64(10240)
	snapMgr := new(SnapManagerBuilder).MaxTotalSize(maxTotalSize).Build(snapFilePath, nil)

	// Add an oldest snapshot for receiving.
	recvKey := SnapKey{RegionID: 100, Term: 100, Index: 100}
	stat := new(SnapStatistics)
	snapData := new(rspb.RaftSnapshotData)
	s, err := snapMgr.GetSnapshotForBuilding(recvKey, dbBundle)
	require.Nil(t, err)
	require.Nil(t, s.Build(dbBundle, genTestRegion(100, 1, 1), snapData, stat, snapMgr))
	recvHead, err := snapData.Marshal()
	require.Nil(t, err)
	s, err = snapMgr.GetSnapshotForSending(recvKey)
	require.Nil(t, err)
	recvRemain, err := ioutil.ReadAll(s)
	require.Nil(t, err)
	assert.True(t, snapMgr.DeleteSnapshot(recvKey, s, true))

	s, err = snapMgr.GetSnapshotForReceiving(recvKey, recvHead)
	require.Nil(t, err)
	_, err = s.Write(recvRemain)
	require.Nil(t, err)
	require.Nil(t, s.Save())

	for i, regionID := range regions {
		key := SnapKey{RegionID: regionID, Term: 1, Index: 1}
		region := genTestRegion(regionID, 1, 1)
		s, err = snapMgr.GetSnapshotForBuilding(key, dbBundle)
		require.Nil(t, err)
		stat = new(SnapStatistics)
		snapData = new(rspb.RaftSnapshotData)
		require.Nil(t, s.Build(dbBundle, region, snapData, stat, snapMgr))

		// TODO: this size may change in different RocksDB version.
		snapSize := uint64(1617)
		maxSnapCount := (maxTotalSize + snapSize - 1) / snapSize
		// The first snap_size is for region 100.
		// That snapshot won't be deleted because it's not for generating.
		assert.Equal(t, snapMgr.GetTotalSnapSize(), snapSize*mathutil.MinUint64(maxSnapCount, uint64(i+2)))
	}
}

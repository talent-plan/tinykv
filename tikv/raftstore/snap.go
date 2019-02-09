package raftstore

import (
	"bytes"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coocood/badger"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/rocksdb"
	"github.com/ngaut/unistore/util"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
)

type CFName = string

const (
	CFDefault CFName = "default"
	CFLock    CFName = "lock"
	CFWrite   CFName = "write"
	CFRaft    CFName = "raft"

	snapGenPrefix       = "gen" // Name prefix for the self-generated snapshot file.
	snapRevPrefix       = "rev" // Name prefix for the received snapshot file.
	sstFileSuffix       = ".sst"
	tmpFileSuffix       = ".tmp"
	cloneFileSuffix     = ".clone"
	metaFileSuffix      = ".meta"
	snapshotVersion     = 2
	deleteRetryMaxTime  = 6
	deleteRetryDuration = 500 * time.Millisecond

	JobStatusPending    uint64 = 0
	JobStatusRunning    uint64 = 1
	JobStatusCancelling uint64 = 2
	JobStatusCancelled  uint64 = 3
	JobStatusFinished   uint64 = 4
	JobStatusFailed     uint64 = 5
)

var (
	snapshotCFs  = []CFName{CFDefault, CFLock, CFWrite}
	defaultCFIdx = 0
	lockCFIdx    = 1
	writeCFIdx   = 2

	errAbort = errors.New("abort")
)

type SnapKey struct {
	RegionID uint64
	Term     uint64
	Index    uint64
}

func (k *SnapKey) String() string {
	return fmt.Sprintf("%d_%d_%d", k.RegionID, k.Term, k.Index)
}

func SnapKeyFromRegionSnap(regionID uint64, snap *raftpb.Snapshot) *SnapKey {
	return &SnapKey{
		RegionID: regionID,
		Term:     snap.Metadata.Term,
		Index:    snap.Metadata.Index,
	}
}

func SnapKeyFromSnap(snap *raftpb.Snapshot) (*SnapKey, error) {
	data := new(rspb.RaftSnapshotData)
	err := data.Unmarshal(snap.Data)
	if err != nil {
		return nil, err
	}
	return SnapKeyFromRegionSnap(data.Region.Id, snap), nil
}

type SnapStatistics struct {
	Size    uint64
	KVCount int
}

type ApplyOptions struct {
	DBBundle  *DBBundle
	Region    *metapb.Region
	Abort     *uint64
	BatchSize int
}

// `Snapshot` is an interface for snapshot.
// It's used in these scenarios:
//   1. build local snapshot
//   2. read local snapshot and then replicate it to remote raftstores
//   3. receive snapshot from remote raftstore and write it to local storage
//   4. apply snapshot
//   5. snapshot gc
type Snapshot interface {
	io.Reader
	io.Writer
	Build(dbBundle *DBBundle, region *metapb.Region, snapData *rspb.RaftSnapshotData,
		stat *SnapStatistics, deleter SnapshotDeleter) error
	Path() string
	Exists() bool
	Delete()
	Meta() (os.FileInfo, error)
	TotalSize() uint64
	Save() error
	Apply(option ApplyOptions) error
}

// copySnapshot is a helper function to copy snapshot.
// Only used in tests.
func copySnapshot(to, from Snapshot) error {
	if !to.Exists() {
		_, err := io.Copy(to, from)
		if err != nil {
			return errors.WithStack(err)
		}
		return to.Save()
	}
	return nil
}

// `SnapshotDeleter` is a trait for deleting snapshot.
// It's used to ensure that the snapshot deletion happens under the protection of locking
// to avoid race case for concurrent read/write.
type SnapshotDeleter interface {
	// DeleteSnapshot returns true if it successfully delete the specified snapshot.
	DeleteSnapshot(key *SnapKey, snapshot Snapshot, checkEntry bool) bool
}

func retryDeleteSnapshot(deleter SnapshotDeleter, key *SnapKey, snap Snapshot) bool {
	for i := 0; i < deleteRetryMaxTime; i++ {
		if deleter.DeleteSnapshot(key, snap, true) {
			return true
		}
		time.Sleep(deleteRetryDuration)
	}
	return false
}

func genSnapshotMeta(cfFiles []*CFFile) (*rspb.SnapshotMeta, error) {
	cfMetas := make([]*rspb.SnapshotCFFile, 0, len(snapshotCFs))
	for _, cfFile := range cfFiles {
		var found bool
		for _, snapCF := range snapshotCFs {
			if snapCF == cfFile.CF {
				found = true
				break
			}
		}
		if !found {
			return nil, errors.Errorf("failed to encode invalid snapshot CF %s", cfFile.CF)
		}
		cfMeta := &rspb.SnapshotCFFile{
			Cf:       cfFile.CF,
			Size_:    cfFile.Size,
			Checksum: cfFile.Checksum,
		}
		cfMetas = append(cfMetas, cfMeta)
	}
	return &rspb.SnapshotMeta{
		CfFiles: cfMetas,
	}, nil
}

func checkFileSize(path string, expectedSize uint64) error {
	size, err := util.GetFileSize(path)
	if err != nil {
		return err
	}
	if size != expectedSize {
		return errors.Errorf("invalid size %d for snapshot cf file %s, expected %d", size, path, expectedSize)
	}
	return nil
}

func checkFileChecksum(path string, expectedChecksum uint32) error {
	checksum, err := util.CalcCRC32(path)
	if err != nil {
		return err
	}
	if checksum != expectedChecksum {
		return errors.Errorf("invalid checksum %d for snapshot cf file %s, expected %d",
			checksum, path, expectedChecksum)
	}
	return nil
}

func checkFileSizeAndChecksum(path string, expectedSize uint64, expectedChecksum uint32) error {
	err := checkFileSize(path, expectedSize)
	if err == nil {
		err = checkFileChecksum(path, expectedChecksum)
	}
	return err
}

type CFFile struct {
	CF          CFName
	Path        string
	TmpPath     string
	ClonePath   string
	SstWriter   *rocksdb.SstFileWriter
	File        *os.File
	KVCount     int
	Size        uint64
	WrittenSize uint64
	Checksum    uint32
	WriteDigest hash.Hash32
}

type MetaFile struct {
	Meta *rspb.SnapshotMeta
	Path string
	File *os.File

	// for writing snapshot
	TmpPath string
}

var _ Snapshot = new(Snap)

type Snap struct {
	key         *SnapKey
	displayPath string
	CFFiles     []*CFFile
	cfIndex     int

	MetaFile     *MetaFile
	SizeTrack    *int64
	limiter      *IOLimiter
	holdTmpFiles bool
}

func NewSnap(dir string, key *SnapKey, sizeTrack *int64, isSending, toBuild bool,
	deleter SnapshotDeleter, limiter *IOLimiter) (*Snap, error) {
	if !util.DirExists(dir) {
		err := os.MkdirAll(dir, 0700)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	var snapPrefix string
	if isSending {
		snapPrefix = snapGenPrefix
	} else {
		snapPrefix = snapRevPrefix
	}
	prefix := fmt.Sprintf("%s_%s", snapPrefix, key)
	displayPath := getDisplayPath(dir, prefix)
	cfFiles := make([]*CFFile, 0, len(snapshotCFs))
	for _, cf := range snapshotCFs {
		fileName := fmt.Sprintf("%s_%s%s", prefix, cf, sstFileSuffix)
		path := filepath.Join(dir, fileName)
		tmpPath := path + tmpFileSuffix
		clonePath := path + cloneFileSuffix
		cfFile := &CFFile{
			CF:        cf,
			Path:      path,
			TmpPath:   tmpPath,
			ClonePath: clonePath,
		}
		cfFiles = append(cfFiles, cfFile)
	}
	metaFileName := fmt.Sprintf("%s%s", prefix, metaFileSuffix)
	metaFilePath := filepath.Join(dir, metaFileName)
	metaTmpPath := metaFilePath + tmpFileSuffix
	metaFile := &MetaFile{
		Path:    metaFilePath,
		TmpPath: metaTmpPath,
	}
	s := &Snap{
		key:         key,
		displayPath: displayPath,
		CFFiles:     cfFiles,
		MetaFile:    metaFile,
		SizeTrack:   sizeTrack,
		limiter:     limiter,
	}

	// load snapshot meta if meta file exists.
	if util.FileExists(metaFile.Path) {
		err := s.loadSnapMeta()
		if err != nil {
			if !toBuild {
				return nil, err
			}
			log.Warnf("failed to load existent snapshot meta when try to build %s: %v", s.Path(), err)
			if !retryDeleteSnapshot(deleter, key, s) {
				log.Warnf("failed to delete snapshot %s because it's already registered elsewhere", s.Path())
				return nil, err
			}
		}
	}
	return s, nil
}

func NewSnapForBuilding(dir string, key *SnapKey, sizeTrack *int64, deleter SnapshotDeleter, limiter *IOLimiter) (*Snap, error) {
	s, err := NewSnap(dir, key, sizeTrack, true, true, deleter, limiter)
	if err != nil {
		return nil, err
	}
	err = s.initForBuilding()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func NewSnapForSending(dir string, key *SnapKey, sizeTrack *int64, deleter SnapshotDeleter) (*Snap, error) {
	s, err := NewSnap(dir, key, sizeTrack, true, false, deleter, nil)
	if err != nil {
		return nil, err
	}
	if !s.Exists() {
		// Skip the initialization below if it doesn't exists.
		return s, nil
	}
	for _, cfFile := range s.CFFiles {
		// initialize cf file size and reader
		if cfFile.Size > 0 {
			cfFile.File, err = os.Open(cfFile.Path)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
	}
	return s, nil
}

func NewSnapForReceiving(dir string, key *SnapKey, snapshotMeta *rspb.SnapshotMeta,
	sizeTrack *int64, deleter SnapshotDeleter, limiter *IOLimiter) (*Snap, error) {
	s, err := NewSnap(dir, key, sizeTrack, false, false, deleter, limiter)
	if err != nil {
		return nil, err
	}
	err = s.setSnapshotMeta(snapshotMeta)
	if err != nil {
		return nil, err
	}
	if s.Exists() {
		return s, nil
	}
	f, err := os.OpenFile(s.MetaFile.TmpPath, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	s.MetaFile.File = f
	s.holdTmpFiles = true

	for _, cfFile := range s.CFFiles {
		if cfFile.Size == 0 {
			continue
		}
		f, err = os.OpenFile(cfFile.TmpPath, os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			return nil, err
		}
		cfFile.File = f
		cfFile.WriteDigest = crc32.NewIEEE()
	}
	return s, nil
}

func NewSnapForApplying(dir string, key *SnapKey, sizeTrack *int64, deleter SnapshotDeleter) (*Snap, error) {
	return NewSnap(dir, key, sizeTrack, false, false, deleter, nil)
}

func (s *Snap) initForBuilding() error {
	if s.Exists() {
		return nil
	}
	file, err := os.OpenFile(s.MetaFile.TmpPath, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	s.MetaFile.File = file
	s.holdTmpFiles = true
	for _, cfFile := range s.CFFiles {
		file, err = os.OpenFile(cfFile.TmpPath, os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		if plainFileUsed(cfFile.CF) {
			cfFile.File = file
		} else {
			opts := rocksdb.NewDefaultBlockBasedTableOptions(bytes.Compare)
			cfFile.SstWriter = rocksdb.NewSstFileWriter(file, opts)
		}
	}
	return nil
}

func plainFileUsed(cf string) bool {
	return cf == CFLock
}

func (s *Snap) readSnapshotMeta() (*rspb.SnapshotMeta, error) {
	fi, err := os.Stat(s.MetaFile.Path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	file, err := os.Open(s.MetaFile.Path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	size := fi.Size()
	buf := make([]byte, size)
	_, err = io.ReadFull(file, buf)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	snapshotMeta := new(rspb.SnapshotMeta)
	err = snapshotMeta.Unmarshal(buf)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return snapshotMeta, nil
}

func (s *Snap) setSnapshotMeta(snapshotMeta *rspb.SnapshotMeta) error {
	if len(snapshotMeta.CfFiles) != len(s.CFFiles) {
		return errors.Errorf("invalid CF number of snapshot meta, expect %d, got %d",
			len(s.CFFiles), len(snapshotMeta.CfFiles))
	}
	for i, cfFile := range s.CFFiles {
		meta := snapshotMeta.CfFiles[i]
		if meta.Cf != cfFile.CF {
			return errors.Errorf("invalid %d CF in snapshot meta, expect %s, got %s", i, cfFile.CF, meta.Cf)
		}
		if util.FileExists(cfFile.Path) {
			// Check only the file size for `exists()` to work correctly.
			err := checkFileSize(cfFile.Path, meta.GetSize_())
			if err != nil {
				return err
			}
		}
		cfFile.Size = uint64(meta.GetSize_())
		cfFile.Checksum = meta.GetChecksum()
	}
	s.MetaFile.Meta = snapshotMeta
	return nil
}

func (s *Snap) loadSnapMeta() error {
	snapshotMeta, err := s.readSnapshotMeta()
	if err != nil {
		return err
	}
	err = s.setSnapshotMeta(snapshotMeta)
	if err != nil {
		return err
	}
	// check if there is a data corruption when the meta file exists
	// but cf files are deleted.
	if !s.Exists() {
		return errors.Errorf("snapshot %s is corrupted, some cf file is missing", s.Path())
	}
	return nil
}

func getDisplayPath(dir string, prefix string) string {
	cfNames := "(" + strings.Join(snapshotCFs, "|") + ")"
	return fmt.Sprintf("%s/%s_%s%s", dir, prefix, cfNames, sstFileSuffix)
}

func (s *Snap) validate(db *badger.DB) error {
	for _, cfFile := range s.CFFiles {
		if cfFile.Size == 0 {
			// Skip empty file. The checksum of this cf file should be 0 and
			// this is checked when loading the snapshot meta.
			continue
		}
		if plainFileUsed(cfFile.CF) {
			err := checkFileSizeAndChecksum(cfFile.Path, cfFile.Size, cfFile.Checksum)
			if err != nil {
				return err
			}
		} else {
			// TODO: prepare and validate for ingestion
		}
	}
	return nil
}

func (s *Snap) saveCFFiles() error {
	for _, cfFile := range s.CFFiles {
		if plainFileUsed(cfFile.CF) {
			cfFile.File.Close()
		} else {
			if cfFile.KVCount > 0 {
				err := cfFile.SstWriter.Finish()
				if err != nil {
					return err
				}
			}
			cfFile.SstWriter.Close()
		}
		size, err := util.GetFileSize(cfFile.TmpPath)
		if err != nil {
			return err
		}
		if size > 0 {
			err = os.Rename(cfFile.TmpPath, cfFile.Path)
			if err != nil {
				return errors.WithStack(err)
			}
			cfFile.Size = size
			// add size
			atomic.AddInt64(s.SizeTrack, int64(size))
			cfFile.Checksum, err = util.CalcCRC32(cfFile.Path)
			if err != nil {
				return err
			}
		} else {
			// Clean up the `tmp_path` if this cf file is empty.
			_, err = util.DeleteFileIfExists(cfFile.TmpPath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Snap) saveMetaFile() error {
	bin, err := s.MetaFile.Meta.Marshal()
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = s.MetaFile.File.Write(bin)
	if err != nil {
		return errors.WithStack(err)
	}
	err = os.Rename(s.MetaFile.TmpPath, s.MetaFile.Path)
	if err != nil {
		return errors.WithStack(err)
	}
	s.holdTmpFiles = false
	return nil
}

func (s *Snap) Build(dbSnap *DBBundle, region *metapb.Region, snapData *rspb.RaftSnapshotData,
	stat *SnapStatistics, deleter SnapshotDeleter) error {
	if s.Exists() {
		err := s.validate(dbSnap.db)
		if err == nil {
			return nil
		}
		log.Errorf("[region %d] file %s is corrupted, will rebuild: %v", region.Id, s.Path(), err)
		if !retryDeleteSnapshot(deleter, s.key, s) {
			log.Errorf("[region %d] failed to delete corrupted snapshot %s because it's already registered elsewhere",
				region.Id, s.Path())
			return err
		}
		err = s.initForBuilding()
		if err != nil {
			return err
		}
	}

	builder, err := newSnapBuilder(s.CFFiles, dbSnap.db, dbSnap.lockStore, region)
	if err != nil {
		return err
	}
	err = builder.build()
	if err != nil {
		return err
	}
	log.Infof("region %d scan snapshot %s, key count %d, size %d", region.Id, s.Path(), builder.kvCount, builder.size)
	err = s.saveCFFiles()
	if err != nil {
		return err
	}
	stat.KVCount = builder.kvCount
	snapshotMeta, err := genSnapshotMeta(s.CFFiles)
	if err != nil {
		return err
	}
	s.MetaFile.Meta = snapshotMeta
	err = s.saveMetaFile()
	if err != nil {
		return err
	}
	totalSize := s.TotalSize()
	stat.Size = totalSize
	// set snapshot meta data
	snapData.FileSize = totalSize
	snapData.Version = snapshotVersion
	snapData.Meta = s.MetaFile.Meta
	return nil
}

func (s *Snap) Path() string {
	return s.displayPath
}

func (s *Snap) Exists() bool {
	for _, cfFile := range s.CFFiles {
		if cfFile.Size > 0 && !util.FileExists(cfFile.Path) {
			return false
		}
	}
	return util.FileExists(s.MetaFile.Path)
}

func (s *Snap) Delete() {
	log.Debugf("deleting %s", s.Path())
	for _, cfFile := range s.CFFiles {
		_, err := util.DeleteFileIfExists(cfFile.ClonePath)
		if err != nil {
			panic(err)
		}
		if s.holdTmpFiles {
			_, err = util.DeleteFileIfExists(cfFile.TmpPath)
			if err != nil {
				panic(err)
			}
		}
		deleted, err := util.DeleteFileIfExists(cfFile.Path)
		if err != nil {
			panic(err)
		}
		if deleted {
			atomic.AddInt64(s.SizeTrack, -int64(cfFile.Size))
		}
	}
	_, err := util.DeleteFileIfExists(s.MetaFile.Path)
	if err != nil {
		panic(err)
	}
	if s.holdTmpFiles {
		_, err := util.DeleteFileIfExists(s.MetaFile.TmpPath)
		if err != nil {
			panic(err)
		}
	}
}

func (s *Snap) Meta() (os.FileInfo, error) {
	fi, err := os.Stat(s.MetaFile.Path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return fi, nil
}

func (s *Snap) TotalSize() (total uint64) {
	for _, cf := range s.CFFiles {
		total += cf.Size
	}
	return
}

func (s *Snap) Save() error {
	log.Debugf("saving to %s", s.MetaFile.Path)
	for _, cfFile := range s.CFFiles {
		if cfFile.Size == 0 {
			// skip empty cf file.
			continue
		}
		// Check each cf file has been fully written, and the checksum matches.
		if cfFile.WrittenSize != cfFile.Size {
			return errors.Errorf("snapshot file %s for CF %s size mismatch, real size %d, expected %d",
				cfFile.Path, cfFile.CF, cfFile.WrittenSize, cfFile.Size)
		}
		checksum := cfFile.WriteDigest.Sum32()
		if cfFile.Checksum != checksum {
			return errors.Errorf("snapshot file %s for CF %s checksum mismatch, real checksum %d, expected %d",
				cfFile.Path, cfFile.CF, checksum, cfFile.Checksum)
		}
		err := os.Rename(cfFile.TmpPath, cfFile.Path)
		if err != nil {
			return errors.WithStack(err)
		}
		atomic.AddInt64(s.SizeTrack, int64(cfFile.Size))
	}
	// write meta file
	bin, err := s.MetaFile.Meta.Marshal()
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = s.MetaFile.File.Write(bin)
	if err != nil {
		return errors.WithStack(err)
	}
	err = s.MetaFile.File.Sync()
	if err != nil {
		return errors.WithStack(err)
	}
	err = os.Rename(s.MetaFile.TmpPath, s.MetaFile.Path)
	if err != nil {
		return errors.WithStack(err)
	}
	s.holdTmpFiles = false
	return nil
}

func (s *Snap) Apply(opts ApplyOptions) error {
	err := s.validate(opts.DBBundle.db)
	if err != nil {
		return err
	}
	err = checkAbort(opts.Abort)
	if err != nil {
		return err
	}
	applier, err := newSnapApplier(s.CFFiles)
	if err != nil {
		return err
	}
	defer applier.close()
	batch := new(WriteBatch)
	for {
		item, err1 := applier.next()
		if err1 != nil {
			return err1
		}
		if item == nil {
			break
		}
		switch item.applySnapType {
		case applySnapTypePut:
			batch.SetWithUserMeta(item.key, item.val, item.useMeta)
			if batch.size >= opts.BatchSize {
				err = batch.WriteToDB(opts.DBBundle.db)
				if err != nil {
					return err
				}
				batch = new(WriteBatch)
			}
		case applySnapTypeLock:
			opts.DBBundle.lockStore.Insert(item.key, item.val)
		case applySnapTypeRollback:
			opts.DBBundle.rollbackStore.Insert(item.key, item.val)
		}
	}
	return batch.WriteToDB(opts.DBBundle.db)
}

func checkAbort(status *uint64) error {
	if atomic.LoadUint64(status) == JobStatusCancelling {
		return errAbort
	}
	return nil
}

func (s *Snap) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	for s.cfIndex < len(s.CFFiles) {
		cfFile := s.CFFiles[s.cfIndex]
		if cfFile.Size == 0 {
			s.cfIndex++
			continue
		}
		n, err := cfFile.File.Read(b)
		if n > 0 {
			return n, nil
		}
		if err != nil {
			if err == io.EOF {
				s.cfIndex++
				continue
			}
			return 0, errors.WithStack(err)
		}
	}
	return 0, io.EOF
}

func (s *Snap) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	nextBuf := b
	for s.cfIndex < len(s.CFFiles) {
		cfFile := s.CFFiles[s.cfIndex]
		if cfFile.Size == 0 {
			s.cfIndex++
			continue
		}
		left := cfFile.Size - cfFile.WrittenSize
		if left == 0 {
			s.cfIndex++
			continue
		}
		file := cfFile.File
		digest := cfFile.WriteDigest
		if len(nextBuf) > int(left) {
			_, err := file.Write(nextBuf[:left])
			if err != nil {
				return 0, errors.WithStack(err)
			}
			digest.Write(nextBuf[:left])
			cfFile.WrittenSize += left
			s.cfIndex++
			nextBuf = nextBuf[left:]
		} else {
			_, err := file.Write(nextBuf)
			if err != nil {
				return 0, errors.WithStack(err)
			}
			digest.Write(nextBuf)
			cfFile.WrittenSize += uint64(len(nextBuf))
			return len(b), nil
		}
	}
	return len(b) - len(nextBuf), nil
}

func (s *Snap) Drop() {
	var cfTmpFileExists bool
	for _, cfFile := range s.CFFiles {
		// cleanup if some of the cf files and meta file is partly written
		if util.FileExists(cfFile.TmpPath) {
			cfTmpFileExists = true
			break
		}
	}
	if cfTmpFileExists || util.FileExists(s.MetaFile.TmpPath) {
		s.Delete()
		return
	}
	// cleanup if data corruption happens and any file goes missing
	if !s.Exists() {
		s.Delete()
	}
}

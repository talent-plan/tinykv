package badger

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/Connor1996/badger/epoch"
	"github.com/Connor1996/badger/fileutil"
	"github.com/Connor1996/badger/y"
	"github.com/ncw/directio"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
)

const blobFileSuffix = ".blob"

type blobPointer struct {
	logicalAddr
	length uint32
}

func (bp *blobPointer) decode(val []byte) {
	ptr := (*blobPointer)(unsafe.Pointer(&val[0]))
	*bp = *ptr
}

type mappingEntry struct {
	logicalAddr
	physicalOffset uint32
}

/*
data format of blob file:
	/ addrMappingLength(4) / addrMappingEntry(12) ... / entry ... / zero (4) / discardInfo ... /

addrMappingEntry:
	/ logicalAddr (8) / physicalOffset(4) /

logicalAddr:
    / logicalFid(4) / logicalOffset(4) /

entry:
	/ value len(4) / value(value len) /

discard info:
	/ logicalAddr(8) ... / totalDiscard(4) / discardInfoLength(4) /
*/
type blobFile struct {
	path           string
	fid            uint32
	fd             *os.File
	fileSize       uint32
	mappingSize    uint32
	mmap           []byte
	mappingEntries []mappingEntry

	// only accessed by gcHandler
	totalDiscard uint32
}

func (bf *blobFile) getID() uint32 {
	if bf == nil {
		return math.MaxUint32
	}
	return bf.fid
}

func (bf *blobFile) loadOffsetMap() error {
	var headBuf [4]byte
	_, err := bf.fd.ReadAt(headBuf[:], 0)
	if err != nil {
		return err
	}
	bf.mappingSize = binary.LittleEndian.Uint32(headBuf[:])
	if bf.mappingSize == 0 {
		return nil
	}
	bf.mmap, err = y.Mmap(bf.fd, false, int64(bf.mappingSize))
	if err != nil {
		return err
	}
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&bf.mappingEntries))
	hdr.Len = int(bf.mappingSize-4) / 12
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&bf.mmap[4]))
	return nil
}

func (bf *blobFile) loadDiscards() error {
	var footBuf [8]byte
	_, err := bf.fd.ReadAt(footBuf[:], int64(bf.fileSize-8))
	if err != nil {
		return err
	}
	bf.totalDiscard = binary.LittleEndian.Uint32(footBuf[:])
	return nil
}

func (bf *blobFile) read(bp blobPointer, s *y.Slice) (buf []byte, err error) {
	physicalOff := int64(bf.getPhysicalOffset(bp.logicalAddr))
	buf = s.Resize(int(bp.length))
	_, err = bf.fd.ReadAt(buf, physicalOff) // skip the 4 bytes length.
	return buf, err
}

func (bf *blobFile) getPhysicalOffset(addr logicalAddr) uint32 {
	if bf.fid == addr.fid {
		return addr.offset
	}
	n := sort.Search(len(bf.mappingEntries), func(i int) bool {
		entry := bf.mappingEntries[i]
		return !entry.logicalAddr.Less(addr)
	})
	return bf.mappingEntries[n].physicalOffset
}

func (bf *blobFile) Delete() error {
	if bf.mmap != nil {
		y.Munmap(bf.mmap)
	}
	bf.fd.Close()
	return os.Remove(bf.path)
}

type blobFileBuilder struct {
	fid    uint32
	file   *os.File
	writer *fileutil.DirectWriter
}

func newBlobFileBuilder(fid uint32, dir string, writeBufferSize int) (*blobFileBuilder, error) {
	fileName := newBlobFileName(fid, dir)
	file, err := directio.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	writer := fileutil.NewDirectWriter(file, writeBufferSize, nil)
	// Write 4 bytes 0 header.
	err = writer.Append(make([]byte, 4))
	if err != nil {
		return nil, err
	}
	return &blobFileBuilder{
		fid:    uint32(fid),
		file:   file,
		writer: writer,
	}, nil
}

func (bfb *blobFileBuilder) append(value []byte) (bp []byte, err error) {
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(value)))
	err = bfb.writer.Append(lenBuf[:])
	if err != nil {
		return
	}
	offset := uint32(bfb.writer.Offset())
	err = bfb.writer.Append(value)
	if err != nil {
		return
	}
	bp = make([]byte, 12)
	binary.LittleEndian.PutUint32(bp, bfb.fid)
	binary.LittleEndian.PutUint32(bp[4:], offset)
	binary.LittleEndian.PutUint32(bp[8:], uint32(len(value)))
	return
}

func (bfb *blobFileBuilder) finish() (*blobFile, error) {
	// Write 4 bytes footer
	err := bfb.writer.Append(make([]byte, 4))
	if err != nil {
		return nil, err
	}
	err = bfb.writer.Finish()
	if err != nil {
		return nil, err
	}
	_ = bfb.file.Close()
	return newBlobFile(bfb.file.Name(), bfb.fid, uint32(bfb.writer.Offset()))
}

func newBlobFile(path string, fid, fileSize uint32) (*blobFile, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	_, err = file.Seek(0, 2)
	if err != nil {
		return nil, err
	}
	return &blobFile{
		path:     path,
		fid:      fid,
		fd:       file,
		fileSize: fileSize,
	}, nil
}

func newBlobFileName(id uint32, dir string) string {
	return filepath.Join(dir, fmt.Sprintf("%08x", id)+blobFileSuffix)
}

type blobManager struct {
	filesLock         sync.RWMutex
	physicalFiles     map[uint32]*blobFile
	logicalToPhysical map[uint32]uint32 // maps logical fid to physical fid.
	changeLog         *os.File
	dirPath           string
	kv                *DB
	discardCh         chan<- *DiscardStats
	maxFileID         uint32
}

func (bm *blobManager) Open(kv *DB, opt Options) error {
	bm.physicalFiles = map[uint32]*blobFile{}
	bm.dirPath = opt.ValueDir
	bm.kv = kv
	validFids, err := bm.loadChangeLogs()
	if err != nil {
		return err
	}
	fileInfos, err := ioutil.ReadDir(bm.dirPath)
	if err != nil {
		return errors.Wrapf(err, "Error while opening blob files")
	}
	for _, fileInfo := range fileInfos {
		if !strings.HasSuffix(fileInfo.Name(), ".blob") {
			continue
		}
		fsz := len(fileInfo.Name())
		fid64, err := strconv.ParseUint(fileInfo.Name()[:fsz-5], 16, 64)
		if err != nil {
			return errors.Wrapf(err, "Error while parsing blob file id for file: %q", fileInfo.Name())
		}
		fid := uint32(fid64)
		path := filepath.Join(bm.dirPath, fileInfo.Name())
		if _, ok := validFids[fid]; !ok {
			_ = os.Remove(path)
			continue
		}
		if _, ok := bm.physicalFiles[fid]; ok {
			return errors.Errorf("Found the same blob file twice: %d", fid)
		}
		blobFile, err := newBlobFile(path, fid, uint32(fileInfo.Size()))
		if err != nil {
			return err
		}
		err = blobFile.loadOffsetMap()
		if err != nil {
			return err
		}
		err = blobFile.loadDiscards()
		if err != nil {
			return err
		}
		bm.physicalFiles[fid] = blobFile
	}
	for _, to := range bm.logicalToPhysical {
		if _, ok := bm.physicalFiles[to]; !ok {
			return errors.Errorf("File %d not found", to)
		}
	}
	discardCh := make(chan *DiscardStats, 1024)
	bm.discardCh = discardCh
	gcHandler := &blobGCHandler{
		bm:                bm,
		discardCh:         discardCh,
		gcCandidate:       map[*blobFile]struct{}{},
		physicalCache:     make(map[uint32]*blobFile, len(bm.physicalFiles)),
		logicalToPhysical: map[uint32]uint32{},
	}
	for k, v := range bm.logicalToPhysical {
		gcHandler.logicalToPhysical[k] = v
	}
	for k, v := range bm.physicalFiles {
		gcHandler.physicalCache[k] = v
	}
	go gcHandler.run()
	return nil
}

func (bm *blobManager) allocFileID() uint32 {
	return atomic.AddUint32(&bm.maxFileID, 1)
}

func (bm *blobManager) read(ptr []byte, s *y.Slice, cache map[uint32]*blobCache) ([]byte, error) {
	var bp blobPointer
	bp.decode(ptr)
	bc, ok := cache[bp.fid]
	if !ok {
		bf := bm.getFile(bp.fid)
		bc = &blobCache{
			file: bf,
		}
		cache[bf.fid] = bc
	}
	return bc.read(bp, s)
}

func (bm *blobManager) getFile(fid uint32) *blobFile {
	bm.filesLock.RLock()
	file, ok := bm.physicalFiles[fid]
	if !ok {
		var physicalID uint32
		physicalID, ok = bm.logicalToPhysical[fid]
		if ok {
			file = bm.physicalFiles[physicalID]
		}
	}
	if file == nil {
		log.Error("failed to get file ", fid)
	}
	bm.filesLock.RUnlock()
	return file
}

func (bm *blobManager) addFile(file *blobFile) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf, file.fid)
	binary.LittleEndian.PutUint32(buf[4:], file.fid)
	_, err := bm.changeLog.Write(buf)
	if err != nil {
		return err
	}
	err = bm.changeLog.Sync()
	if err != nil {
		return err
	}
	bm.filesLock.Lock()
	bm.physicalFiles[file.fid] = file
	bm.filesLock.Unlock()
	return nil
}

func (bm *blobManager) addGCFile(oldFiles []*blobFile, newFile *blobFile, logicalFiles map[uint32]struct{}, guard *epoch.Guard) error {
	oldFids := make([]uint32, len(oldFiles))
	for i, v := range oldFiles {
		oldFids[i] = v.fid
	}
	log.Infof("addGCFile old files %v, new file id %d, logical files %v", oldFids, newFile.getID(), logicalFiles)
	buf := make([]byte, len(oldFiles)*8)
	for i, oldFile := range oldFiles {
		offset := i * 8
		binary.LittleEndian.PutUint32(buf[offset:], oldFile.fid)
		binary.LittleEndian.PutUint32(buf[offset+4:], newFile.getID())
	}
	_, err := bm.changeLog.Write(buf)
	if err != nil {
		return err
	}
	err = bm.changeLog.Sync()
	if err != nil {
		return err
	}
	bm.filesLock.Lock()
	if newFile != nil {
		bm.physicalFiles[newFile.fid] = newFile
		for logicalFid := range logicalFiles {
			bm.logicalToPhysical[logicalFid] = newFile.fid
		}
	} else {
		for logicalFid := range logicalFiles {
			delete(bm.logicalToPhysical, logicalFid)
		}
	}
	for _, old := range oldFiles {
		delete(bm.physicalFiles, old.fid)
	}
	bm.filesLock.Unlock()
	del := make([]epoch.Resource, len(oldFids))
	for i := range oldFiles {
		del[i] = oldFiles[i]
	}
	guard.Delete(del)
	return nil
}

type fidNode struct {
	fid  uint32
	next *fidNode
}

func (bm *blobManager) loadChangeLogs() (validFids map[uint32]struct{}, err error) {
	changeLogFileName := filepath.Join(bm.dirPath, "blob_change.log")
	data, err := ioutil.ReadFile(changeLogFileName)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	validFids = bm.buildLogicalToPhysical(data)
	bm.changeLog, err = os.OpenFile(changeLogFileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	_, err = bm.changeLog.Seek(0, 2)
	if err != nil {
		return nil, err
	}
	return validFids, nil
}

func (bm *blobManager) buildLogicalToPhysical(data []byte) (validFids map[uint32]struct{}) {
	changeLogMap := map[uint32]uint32{} // maps old fid to a new fid.
	logicalFids := map[uint32]struct{}{}
	for i := 0; i < len(data); i += 8 {
		fromFid := binary.LittleEndian.Uint32(data[i:])
		toFid := binary.LittleEndian.Uint32(data[i+4:])
		changeLogMap[fromFid] = toFid
		if fromFid == toFid {
			logicalFids[fromFid] = struct{}{}
		}
	}
	bm.logicalToPhysical = map[uint32]uint32{}
	validFids = map[uint32]struct{}{}
	for fid := range logicalFids {
		toFid := getToFid(changeLogMap, fid)
		if toFid != math.MaxUint32 {
			bm.logicalToPhysical[fid] = toFid
			validFids[toFid] = struct{}{}
			if bm.maxFileID < toFid {
				bm.maxFileID = toFid
			}
		}
	}
	return
}

func getToFid(logicalMap map[uint32]uint32, toFid uint32) uint32 {
	for {
		nextToFid, ok := logicalMap[toFid]
		if !ok {
			return toFid
		}
		if nextToFid == toFid {
			return toFid
		}
		toFid = nextToFid
	}
}

type blobGCHandler struct {
	bm                *blobManager
	discardCh         <-chan *DiscardStats
	physicalCache     map[uint32]*blobFile
	logicalToPhysical map[uint32]uint32

	gcCandidate          map[*blobFile]struct{}
	candidateValidSize   uint32
	candidateDiscardSize uint64
}

func (h *blobGCHandler) run() {
	for discardInfo := range h.discardCh {
		h.handleDiscardInfo(discardInfo)
		err := h.doGCIfNeeded()
		if err != nil {
			log.Error(err)
		}
	}
}

func (h *blobGCHandler) handleDiscardInfo(discardStats *DiscardStats) {
	physicalDiscards := make(map[uint32][]blobPointer)
	for _, ptr := range discardStats.ptrs {
		physicalFid := h.getLogicalToPhysical(ptr.fid)
		ptrs := physicalDiscards[physicalFid]
		physicalDiscards[physicalFid] = append(ptrs, ptr)
	}
	for physicalFid, ptrs := range physicalDiscards {
		err := h.writeDiscardToFile(physicalFid, ptrs)
		if err != nil {
			log.Error("handleDiscardInfo", physicalFid, err)
			continue
		}
	}
}

func (h *blobGCHandler) getPhysicalFile(physicalFid uint32) *blobFile {
	file := h.physicalCache[physicalFid]
	if file == nil {
		file = h.bm.getFile(physicalFid)
		h.physicalCache[physicalFid] = file
	}
	return file
}

func (h *blobGCHandler) getLogicalToPhysical(logicalFid uint32) uint32 {
	physicalFid, ok := h.logicalToPhysical[logicalFid]
	if !ok {
		// must be newly added L0 blob
		h.logicalToPhysical[logicalFid] = logicalFid
		physicalFid = logicalFid
	}
	return physicalFid
}

func (h *blobGCHandler) writeDiscardToFile(physicalFid uint32, ptrs []blobPointer) error {
	file := h.getPhysicalFile(physicalFid)
	discardInfo := make([]byte, uint32(len(ptrs)*8+8))
	totalDiscard := file.totalDiscard + uint32(len(discardInfo))
	for i, ptr := range ptrs {
		binary.LittleEndian.PutUint32(discardInfo[i*8:], ptr.fid)
		binary.LittleEndian.PutUint32(discardInfo[i*8+4:], ptr.offset)
		totalDiscard += ptr.length
	}
	binary.LittleEndian.PutUint32(discardInfo[len(discardInfo)-8:], totalDiscard)
	binary.LittleEndian.PutUint32(discardInfo[len(discardInfo)-4:], uint32(len(discardInfo)))
	_, err := file.fd.Write(discardInfo)
	if err != nil {
		return err
	}
	file.totalDiscard = totalDiscard
	file.fileSize += uint32(len(discardInfo))
	if file.totalDiscard > file.fileSize/2 {
		h.gcCandidate[file] = struct{}{}
		h.candidateValidSize += file.fileSize - file.mappingSize - file.totalDiscard
		h.candidateDiscardSize += uint64(file.totalDiscard)
	}
	return nil
}

var (
	minCandidateValidSize   uint32 = 32 * 1024 * 1024
	maxCandidateValidSize   uint32 = 128 * 1024 * 1024
	maxCandidateDiscardSize uint64 = 512 * 1024 * 1024
)

func (h *blobGCHandler) doGCIfNeeded() error {
	guard := h.bm.kv.resourceMgr.Acquire()
	defer guard.Done()

	if len(h.gcCandidate) == 0 {
		return nil
	}
	if h.candidateValidSize < minCandidateValidSize && h.candidateDiscardSize < maxCandidateDiscardSize {
		return nil
	}
	var oldFiles []*blobFile
	var totalValidSize uint32
	for candidate := range h.gcCandidate {
		validSize := candidate.fileSize - candidate.mappingSize - candidate.totalDiscard
		if totalValidSize+validSize > maxCandidateValidSize {
			break
		}
		totalValidSize += validSize
		oldFiles = append(oldFiles, candidate)
		delete(h.gcCandidate, candidate)
	}
	var validEntries []validEntry
	for _, blobFile := range oldFiles {
		blobBytes, err := ioutil.ReadFile(blobFile.path)
		if err != nil {
			return err
		}
		validEntries = h.extractValidEntries(validEntries, blobFile, blobBytes)
	}
	if len(validEntries) == 0 {
		for _, oldFile := range oldFiles {
			delete(h.physicalCache, oldFile.fid)
		}
		return h.bm.addGCFile(oldFiles, nil, nil, guard)
	}
	sort.Slice(validEntries, func(i, j int) bool {
		return validEntries[i].logicalAddr.Less(validEntries[j].logicalAddr)
	})
	newFid := h.bm.allocFileID()
	fileName := newBlobFileName(newFid, h.bm.kv.opt.Dir)
	file, err := directio.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	writer := fileutil.NewDirectWriter(file, 1024*1024, nil)
	// 4 bytes addrMapping length
	mappingSize := 4 + uint32(len(validEntries))*12
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, mappingSize)
	err = writer.Append(lenBuf)
	if err != nil {
		return err
	}
	mappingEntryBuf := make([]byte, 12)
	newOffset := 4 + uint32(len(validEntries))*12 + 4
	logicalFids := make(map[uint32]struct{})
	for _, entry := range validEntries {
		logicalFids[entry.fid] = struct{}{}
		binary.LittleEndian.PutUint32(mappingEntryBuf, entry.fid)
		binary.LittleEndian.PutUint32(mappingEntryBuf[4:], entry.offset)
		binary.LittleEndian.PutUint32(mappingEntryBuf[8:], newOffset)
		newOffset += uint32(len(entry.value)) + 4
		err = writer.Append(mappingEntryBuf)
		if err != nil {
			return err
		}
	}
	for _, entry := range validEntries {
		binary.LittleEndian.PutUint32(lenBuf, uint32(len(entry.value)))
		err = writer.Append(lenBuf)
		if err != nil {
			return err
		}
		err = writer.Append(entry.value)
		if err != nil {
			return err
		}
	}
	// 4 bytes 0 discard length
	err = writer.Append(make([]byte, 4))
	if err != nil {
		return err
	}
	err = writer.Finish()
	if err != nil {
		return err
	}
	file.Close()
	blobFile, err := newBlobFile(file.Name(), newFid, uint32(writer.Offset()))
	if err != nil {
		return err
	}
	err = blobFile.loadOffsetMap()
	if err != nil {
		return err
	}
	h.physicalCache[newFid] = blobFile
	for _, oldFile := range oldFiles {
		delete(h.physicalCache, oldFile.fid)
	}
	for logicalFid := range logicalFids {
		h.logicalToPhysical[logicalFid] = newFid
	}
	return h.bm.addGCFile(oldFiles, blobFile, logicalFids, guard)
}

type logicalAddr struct {
	fid    uint32
	offset uint32
}

func (a logicalAddr) Less(b logicalAddr) bool {
	if a.fid == b.fid {
		return a.offset < b.offset
	}
	return a.fid < b.fid
}

type validEntry struct {
	logicalAddr
	value []byte
}

func (h *blobGCHandler) extractValidEntries(validEntries []validEntry, file *blobFile, blobBytes []byte) []validEntry {
	physicalToLogical := make(map[uint32]logicalAddr, len(file.mappingEntries))
	for _, mappingEntry := range file.mappingEntries {
		physicalToLogical[mappingEntry.physicalOffset] = mappingEntry.logicalAddr
	}
	discardedPhysicalOffsets, endOff := h.buildDiscardPhysicalOffsets(file, blobBytes)
	cursor := file.mappingSize
	for cursor < endOff {
		valLen := binary.LittleEndian.Uint32(blobBytes[cursor:])
		cursor += 4
		physicalOff := cursor
		cursor += valLen
		_, isDiscarded := discardedPhysicalOffsets[physicalOff]
		if isDiscarded {
			continue
		}
		var logical logicalAddr
		if len(file.mappingEntries) == 0 {
			logical.fid = file.fid
			logical.offset = physicalOff
		} else {
			logical = physicalToLogical[physicalOff]
		}
		validEntries = append(validEntries, validEntry{
			value:       blobBytes[physicalOff : physicalOff+valLen],
			logicalAddr: logical,
		})
	}
	return validEntries
}

func (h *blobGCHandler) buildDiscardPhysicalOffsets(file *blobFile, blobBytes []byte) (discards map[uint32]struct{}, endOff uint32) {
	discards = make(map[uint32]struct{})
	blobBytesOff := uint32(len(blobBytes))
	for {
		discardLength := binary.LittleEndian.Uint32(blobBytes[blobBytesOff-4:])
		if discardLength == 0 {
			break
		}
		discardAddrs := blobBytes[blobBytesOff-discardLength : blobBytesOff-8]
		blobBytesOff -= discardLength
		for i := 0; i < len(discardAddrs); i += 8 {
			var addr logicalAddr
			addr.fid = binary.LittleEndian.Uint32(discardAddrs[i:])
			addr.offset = binary.LittleEndian.Uint32(discardAddrs[i+4:])
			physicalOffset := file.getPhysicalOffset(addr)
			discards[physicalOffset] = struct{}{}
		}
	}
	return discards, blobBytesOff
}

type blobCache struct {
	file         *blobFile
	cacheData    []byte
	cacheOffset  uint32
	lastPhysical uint32
}

const cacheSize = 64 * 1024

func (bc *blobCache) read(bp blobPointer, slice *y.Slice) ([]byte, error) {
	physicalOffset := bc.file.getPhysicalOffset(bp.logicalAddr)
	lastPhysical := bc.lastPhysical
	bc.lastPhysical = physicalOffset
	if lastPhysical == 0 || bp.length > cacheSize/2 {
		return bc.file.read(bp, slice)
	}
	if physicalOffset >= bc.cacheOffset && physicalOffset+bp.length < bc.cacheOffset+uint32(len(bc.cacheData)) {
		off := physicalOffset - bc.cacheOffset
		return bc.cacheData[off : off+bp.length], nil
	}
	if bc.cacheData == nil {
		bc.cacheData = make([]byte, cacheSize)
	}
	readLen := uint32(len(bc.cacheData))
	if readLen > bc.file.fileSize-physicalOffset {
		readLen = bc.file.fileSize - physicalOffset
	}
	_, err := bc.file.fd.ReadAt(bc.cacheData[:readLen], int64(physicalOffset))
	if err != nil {
		return nil, err
	}
	bc.cacheOffset = physicalOffset
	return bc.cacheData[:bp.length], nil
}

package runner

import (
	"encoding/hex"

	"github.com/coocood/badger"
	"github.com/ngaut/log"
	"github.com/pingcap-incubator/tinykv/kv/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/tikv/config"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/tikv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/tikv/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap/tidb/util/codec"
)

type SplitCheckTask struct {
	Region *metapb.Region
}

type splitCheckHandler struct {
	engine  *badger.DB
	router  message.RaftRouter
	config  *config.SplitCheckConfig
	checker *sizeSplitChecker
}

func NewSplitCheckHandler(engine *badger.DB, router message.RaftRouter, config *config.SplitCheckConfig) *splitCheckHandler {
	runner := &splitCheckHandler{
		engine:  engine,
		router:  router,
		config:  config,
		checker: newSizeSplitChecker(config.RegionMaxSize, config.RegionSplitSize, config.BatchSplitLimit),
	}
	return runner
}

/// run checks a region with split checkers to produce split keys and generates split admin command.
func (r *splitCheckHandler) Handle(t worker.Task) {
	spCheckTask := t.Data.(*SplitCheckTask)
	region := spCheckTask.Region
	regionId := region.Id
	_, startKey, err := codec.DecodeBytes(region.StartKey, nil)
	if err != nil {
		log.Errorf("failed to decode region key %x, err:%v", region.StartKey, err)
		return
	}
	_, endKey, err := codec.DecodeBytes(region.EndKey, nil)
	if err != nil {
		log.Errorf("failed to decode region key %x, err:%v", region.EndKey, err)
		return
	}
	log.Debugf("executing split check worker.Task: [regionId: %d, startKey: %s, endKey: %s]", regionId,
		hex.EncodeToString(startKey), hex.EncodeToString(endKey))
	keys := r.splitCheck(startKey, endKey)
	if len(keys) != 0 {
		regionEpoch := region.GetRegionEpoch()
		for i, k := range keys {
			keys[i] = codec.EncodeBytes(nil, k)
		}
		msg := message.Msg{
			Type:     message.MsgTypeSplitRegion,
			RegionID: regionId,
			Data: &message.MsgSplitRegion{
				RegionEpoch: regionEpoch,
				SplitKeys:   keys,
			},
		}
		err := r.router.Send(regionId, msg)
		if err != nil {
			log.Warnf("failed to send check result: [regionId: %d, err: %v]", regionId, err)
		}
	} else {
		log.Debugf("no need to send, split key not found: [regionId: %v]", regionId)
	}
}

/// SplitCheck gets the split keys by scanning the range.
func (r *splitCheckHandler) splitCheck(startKey, endKey []byte) [][]byte {
	txn := r.engine.NewTransaction(false)
	defer txn.Discard()

	it := engine_util.NewCFIterator(engine_util.CfDefault, txn)
	defer it.Close()
	for it.Seek(startKey); it.Valid(); it.Next() {
		item := it.Item()
		key := item.Key()
		if engine_util.ExceedEndKey(key, endKey) {
			break
		}
		if r.checker.onKv(key, item) {
			break
		}
	}
	keys := r.checker.getSplitKeys()
	if len(keys) > 0 {
		return keys
	}
	return nil
}

type sizeSplitChecker struct {
	maxSize         uint64
	splitSize       uint64
	currentSize     uint64
	splitKeys       [][]byte
	batchSplitLimit uint64
}

func newSizeSplitChecker(maxSize, splitSize, batchSplitLimit uint64) *sizeSplitChecker {
	return &sizeSplitChecker{
		maxSize:         maxSize,
		splitSize:       splitSize,
		batchSplitLimit: batchSplitLimit,
	}
}

func (checker *sizeSplitChecker) onKv(key []byte, item engine_util.DBItem) bool {
	valueSize := uint64(item.ValueSize())
	size := uint64(len(key)) + valueSize
	checker.currentSize += size
	overLimit := uint64(len(checker.splitKeys)) >= checker.batchSplitLimit
	if checker.currentSize > checker.splitSize && !overLimit {
		checker.splitKeys = append(checker.splitKeys, util.SafeCopy(key))
		// If for previous onKv(), checker.current_size == checker.split_size,
		// the split key would be pushed this time, but the entry size for this time should not be ignored.
		if checker.currentSize-size == checker.splitSize {
			checker.currentSize = size
		} else {
			checker.currentSize = 0
		}
		overLimit = uint64(len(checker.splitKeys)) >= checker.batchSplitLimit
	}
	// For a large region, scan over the range maybe cost too much time,
	// so limit the number of produced splitKeys for one batch.
	// Also need to scan over checker.maxSize for last part.
	return overLimit && checker.currentSize+checker.splitSize >= checker.maxSize
}

func (checker *sizeSplitChecker) getSplitKeys() [][]byte {
	// Make sure not to split when less than maxSize for last part
	if checker.currentSize+checker.splitSize < checker.maxSize {
		splitKeyLen := len(checker.splitKeys)
		if splitKeyLen != 0 {
			checker.splitKeys = checker.splitKeys[:splitKeyLen-1]
		}
	}
	keys := checker.splitKeys
	checker.splitKeys = nil
	return keys
}

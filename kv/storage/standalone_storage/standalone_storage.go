package standalone_storage

import (
	"errors"
	"log"

	"github.com/tecbot/gorocksdb"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
}

// opendb
func OpenDB(conf *config.Config) (*gorocksdb.DB, error) {
	options := gorocksdb.NewDefaultOptions()
	options.SetCreateIfMissing(true)

	bloomFilter := gorocksdb.NewBloomFilter(10)

	readOptions := gorocksdb.NewDefaultReadOptions()
	readOptions.SetFillCache(false)

	rateLimiter := gorocksdb.NewRateLimiter(10000000, 10000, 10)
	options.SetRateLimiter(rateLimiter)
	options.SetCreateIfMissing(true)
	options.EnableStatistics()
	options.SetWriteBufferSize(8 * 1024)
	options.SetMaxWriteBufferNumber(3)
	options.SetMaxBackgroundCompactions(10)
	// options.SetCompression(gorocksdb.SnappyCompression)
	// options.SetCompactionStyle(gorocksdb.UniversalCompactionStyle)

	options.SetHashSkipListRep(2000000, 4, 4)

	blockBasedTableOptions := gorocksdb.NewDefaultBlockBasedTableOptions()
	blockBasedTableOptions.SetBlockCache(gorocksdb.NewLRUCache(64 * 1024))
	blockBasedTableOptions.SetFilterPolicy(bloomFilter)
	blockBasedTableOptions.SetBlockSizeDeviation(5)
	blockBasedTableOptions.SetBlockRestartInterval(10)
	blockBasedTableOptions.SetBlockCacheCompressed(gorocksdb.NewLRUCache(64 * 1024))
	blockBasedTableOptions.SetCacheIndexAndFilterBlocks(true)
	blockBasedTableOptions.SetIndexType(gorocksdb.KHashSearchIndexType)

	options.SetBlockBasedTableFactory(blockBasedTableOptions)
	//log.Println(bloomFilter, readOptions)
	options.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(3))

	options.SetAllowConcurrentMemtableWrites(false)

	db, err := gorocksdb.OpenDb(options, conf.DBPath)

	if err != nil {
		log.Fatalln("OPEN DB error", db, err)
		db.Close()
		return nil, errors.New("fail to open db")
	} else {
		log.Println("OPEN DB success", db)
	}
	return db, nil
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return nil, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	return nil
}

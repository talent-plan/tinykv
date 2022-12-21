// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build rocksdb

package rocksdb

import (
	"context"
	"fmt"
	"os"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/tecbot/gorocksdb"
)

// properties
const (
	rocksdbDir = "rocksdb.dir"
	// DBOptions
	rocksdbAllowConcurrentMemtableWrites   = "rocksdb.allow_concurrent_memtable_writes"
	rocsdbAllowMmapReads                   = "rocksdb.allow_mmap_reads"
	rocksdbAllowMmapWrites                 = "rocksdb.allow_mmap_writes"
	rocksdbArenaBlockSize                  = "rocksdb.arena_block_size"
	rocksdbDBWriteBufferSize               = "rocksdb.db_write_buffer_size"
	rocksdbHardPendingCompactionBytesLimit = "rocksdb.hard_pending_compaction_bytes_limit"
	rocksdbLevel0FileNumCompactionTrigger  = "rocksdb.level0_file_num_compaction_trigger"
	rocksdbLevel0SlowdownWritesTrigger     = "rocksdb.level0_slowdown_writes_trigger"
	rocksdbLevel0StopWritesTrigger         = "rocksdb.level0_stop_writes_trigger"
	rocksdbMaxBytesForLevelBase            = "rocksdb.max_bytes_for_level_base"
	rocksdbMaxBytesForLevelMultiplier      = "rocksdb.max_bytes_for_level_multiplier"
	rocksdbMaxTotalWalSize                 = "rocksdb.max_total_wal_size"
	rocksdbMemtableHugePageSize            = "rocksdb.memtable_huge_page_size"
	rocksdbNumLevels                       = "rocksdb.num_levels"
	rocksdbUseDirectReads                  = "rocksdb.use_direct_reads"
	rocksdbUseFsync                        = "rocksdb.use_fsync"
	rocksdbWriteBufferSize                 = "rocksdb.write_buffer_size"
	rocksdbMaxWriteBufferNumber            = "rocksdb.max_write_buffer_number"
	// TableOptions/BlockBasedTable
	rocksdbBlockSize                        = "rocksdb.block_size"
	rocksdbBlockSizeDeviation               = "rocksdb.block_size_deviation"
	rocksdbCacheIndexAndFilterBlocks        = "rocksdb.cache_index_and_filter_blocks"
	rocksdbNoBlockCache                     = "rocksdb.no_block_cache"
	rocksdbPinL0FilterAndIndexBlocksInCache = "rocksdb.pin_l0_filter_and_index_blocks_in_cache"
	rocksdbWholeKeyFiltering                = "rocksdb.whole_key_filtering"
	rocksdbBlockRestartInterval             = "rocksdb.block_restart_interval"
	rocksdbFilterPolicy                     = "rocksdb.filter_policy"
	rocksdbIndexType                        = "rocksdb.index_type"
	rocksdbWALDir                           = "rocksdb.wal_dir"
	// TODO: add more configurations
)

type rocksDBCreator struct{}

type rocksDB struct {
	p *properties.Properties

	db *gorocksdb.DB

	r       *util.RowCodec
	bufPool *util.BufPool

	readOpts  *gorocksdb.ReadOptions
	writeOpts *gorocksdb.WriteOptions
}

type contextKey string

func (c rocksDBCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	dir := p.GetString(rocksdbDir, "/tmp/rocksdb")

	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		os.RemoveAll(dir)
	}

	opts := getOptions(p)

	db, err := gorocksdb.OpenDb(opts, dir)
	if err != nil {
		return nil, err
	}

	return &rocksDB{
		p:         p,
		db:        db,
		r:         util.NewRowCodec(p),
		bufPool:   util.NewBufPool(),
		readOpts:  gorocksdb.NewDefaultReadOptions(),
		writeOpts: gorocksdb.NewDefaultWriteOptions(),
	}, nil
}

func getTableOptions(p *properties.Properties) *gorocksdb.BlockBasedTableOptions {
	tblOpts := gorocksdb.NewDefaultBlockBasedTableOptions()

	tblOpts.SetBlockSize(p.GetInt(rocksdbBlockSize, 4<<10))
	tblOpts.SetBlockSizeDeviation(p.GetInt(rocksdbBlockSizeDeviation, 10))
	tblOpts.SetCacheIndexAndFilterBlocks(p.GetBool(rocksdbCacheIndexAndFilterBlocks, false))
	tblOpts.SetNoBlockCache(p.GetBool(rocksdbNoBlockCache, false))
	tblOpts.SetPinL0FilterAndIndexBlocksInCache(p.GetBool(rocksdbPinL0FilterAndIndexBlocksInCache, false))
	tblOpts.SetWholeKeyFiltering(p.GetBool(rocksdbWholeKeyFiltering, true))
	tblOpts.SetBlockRestartInterval(p.GetInt(rocksdbBlockRestartInterval, 16))

	if b := p.GetString(rocksdbFilterPolicy, ""); len(b) > 0 {
		if b == "rocksdb.BuiltinBloomFilter" {
			const defaultBitsPerKey = 10
			tblOpts.SetFilterPolicy(gorocksdb.NewBloomFilter(defaultBitsPerKey))
		}
	}

	indexType := p.GetString(rocksdbIndexType, "kBinarySearch")
	if indexType == "kBinarySearch" {
		tblOpts.SetIndexType(gorocksdb.KBinarySearchIndexType)
	} else if indexType == "kHashSearch" {
		tblOpts.SetIndexType(gorocksdb.KHashSearchIndexType)
	} else if indexType == "kTwoLevelIndexSearch" {
		tblOpts.SetIndexType(gorocksdb.KTwoLevelIndexSearchIndexType)
	}

	return tblOpts
}

func getOptions(p *properties.Properties) *gorocksdb.Options {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	opts.SetAllowConcurrentMemtableWrites(p.GetBool(rocksdbAllowConcurrentMemtableWrites, true))
	opts.SetAllowMmapReads(p.GetBool(rocsdbAllowMmapReads, false))
	opts.SetAllowMmapWrites(p.GetBool(rocksdbAllowMmapWrites, false))
	opts.SetArenaBlockSize(p.GetInt(rocksdbArenaBlockSize, 0))
	opts.SetDbWriteBufferSize(p.GetInt(rocksdbDBWriteBufferSize, 0))
	opts.SetHardPendingCompactionBytesLimit(p.GetUint64(rocksdbHardPendingCompactionBytesLimit, 256<<30))
	opts.SetLevel0FileNumCompactionTrigger(p.GetInt(rocksdbLevel0FileNumCompactionTrigger, 4))
	opts.SetLevel0SlowdownWritesTrigger(p.GetInt(rocksdbLevel0SlowdownWritesTrigger, 20))
	opts.SetLevel0StopWritesTrigger(p.GetInt(rocksdbLevel0StopWritesTrigger, 36))
	opts.SetMaxBytesForLevelBase(p.GetUint64(rocksdbMaxBytesForLevelBase, 256<<20))
	opts.SetMaxBytesForLevelMultiplier(p.GetFloat64(rocksdbMaxBytesForLevelMultiplier, 10))
	opts.SetMaxTotalWalSize(p.GetUint64(rocksdbMaxTotalWalSize, 0))
	opts.SetMemtableHugePageSize(p.GetInt(rocksdbMemtableHugePageSize, 0))
	opts.SetNumLevels(p.GetInt(rocksdbNumLevels, 7))
	opts.SetUseDirectReads(p.GetBool(rocksdbUseDirectReads, false))
	opts.SetUseFsync(p.GetBool(rocksdbUseFsync, false))
	opts.SetWriteBufferSize(p.GetInt(rocksdbWriteBufferSize, 64<<20))
	opts.SetMaxWriteBufferNumber(p.GetInt(rocksdbMaxWriteBufferNumber, 2))
	opts.SetWalDir(p.GetString(rocksdbWALDir, ""))

	opts.SetBlockBasedTableFactory(getTableOptions(p))

	return opts
}

func (db *rocksDB) Close() error {
	db.db.Close()
	return nil
}

func (db *rocksDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *rocksDB) CleanupThread(_ context.Context) {
}

func (db *rocksDB) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

func cloneValue(v *gorocksdb.Slice) []byte {
	return append([]byte(nil), v.Data()...)
}

func (db *rocksDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	value, err := db.db.Get(db.readOpts, db.getRowKey(table, key))
	if err != nil {
		return nil, err
	}
	defer value.Free()

	return db.r.Decode(cloneValue(value), fields)
}

func (db *rocksDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	res := make([]map[string][]byte, count)
	it := db.db.NewIterator(db.readOpts)
	defer it.Close()

	rowStartKey := db.getRowKey(table, startKey)

	it.Seek(rowStartKey)
	i := 0
	for it = it; it.Valid() && i < count; it.Next() {
		value := it.Value()
		m, err := db.r.Decode(cloneValue(value), fields)
		if err != nil {
			return nil, err
		}
		res[i] = m
		i++
	}

	if err := it.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func (db *rocksDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	m, err := db.Read(ctx, table, key, nil)
	if err != nil {
		return err
	}

	for field, value := range values {
		m[field] = value
	}

	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	buf, err = db.r.Encode(buf, m)
	if err != nil {
		return err
	}

	rowKey := db.getRowKey(table, key)

	return db.db.Put(db.writeOpts, rowKey, buf)
}

func (db *rocksDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	rowKey := db.getRowKey(table, key)

	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	buf, err := db.r.Encode(buf, values)
	if err != nil {
		return err
	}
	return db.db.Put(db.writeOpts, rowKey, buf)
}

func (db *rocksDB) Delete(ctx context.Context, table string, key string) error {
	rowKey := db.getRowKey(table, key)

	return db.db.Delete(db.writeOpts, rowKey)
}

func init() {
	ycsb.RegisterDBCreator("rocksdb", rocksDBCreator{})
}

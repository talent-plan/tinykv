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

/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package boltdb

import (
	"context"
	"fmt"
	"os"

	"github.com/boltdb/bolt"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// properties
const (
	boltPath            = "bolt.path"
	boltTimeout         = "bolt.timeout"
	boltNoGrowSync      = "bolt.no_grow_sync"
	boltReadOnly        = "bolt.read_only"
	boltMmapFlags       = "bolt.mmap_flags"
	boltInitialMmapSize = "bolt.initial_mmap_size"
)

type boltCreator struct {
}

type boltOptions struct {
	Path      string
	FileMode  os.FileMode
	DBOptions *bolt.Options
}

type boltDB struct {
	p *properties.Properties

	db *bolt.DB

	r       *util.RowCodec
	bufPool *util.BufPool
}

func (c boltCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	opts := getOptions(p)

	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		os.RemoveAll(opts.Path)
	}

	db, err := bolt.Open(opts.Path, opts.FileMode, opts.DBOptions)
	if err != nil {
		return nil, err
	}

	return &boltDB{
		p:       p,
		db:      db,
		r:       util.NewRowCodec(p),
		bufPool: util.NewBufPool(),
	}, nil
}

func getOptions(p *properties.Properties) boltOptions {
	path := p.GetString(boltPath, "/tmp/boltdb")

	opts := bolt.DefaultOptions
	opts.Timeout = p.GetDuration(boltTimeout, 0)
	opts.NoGrowSync = p.GetBool(boltNoGrowSync, false)
	opts.ReadOnly = p.GetBool(boltReadOnly, false)
	opts.MmapFlags = p.GetInt(boltMmapFlags, 0)
	opts.InitialMmapSize = p.GetInt(boltInitialMmapSize, 0)

	return boltOptions{
		Path:      path,
		FileMode:  0600,
		DBOptions: opts,
	}
}

func (db *boltDB) Close() error {
	return db.db.Close()
}

func (db *boltDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *boltDB) CleanupThread(_ context.Context) {
}

func (db *boltDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var m map[string][]byte
	err := db.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(table))
		if bucket == nil {
			return fmt.Errorf("table not found: %s", table)
		}

		row := bucket.Get([]byte(key))
		if row == nil {
			return fmt.Errorf("key not found: %s.%s", table, key)
		}

		var err error
		m, err = db.r.Decode(row, fields)
		return err
	})
	return m, err
}

func (db *boltDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	res := make([]map[string][]byte, count)
	err := db.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(table))
		if bucket == nil {
			return fmt.Errorf("table not found: %s", table)
		}

		cursor := bucket.Cursor()
		key, value := cursor.Seek([]byte(startKey))
		for i := 0; key != nil && i < count; i++ {
			m, err := db.r.Decode(value, fields)
			if err != nil {
				return err
			}

			res[i] = m
			key, value = cursor.Next()
		}

		return nil
	})
	return res, err
}

func (db *boltDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	err := db.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(table))
		if bucket == nil {
			return fmt.Errorf("table not found: %s", table)
		}

		value := bucket.Get([]byte(key))
		if value == nil {
			return fmt.Errorf("key not found: %s.%s", table, key)
		}

		data, err := db.r.Decode(value, nil)
		if err != nil {
			return err
		}

		for field, value := range values {
			data[field] = value
		}

		buf := db.bufPool.Get()
		defer func() {
			db.bufPool.Put(buf)
		}()

		buf, err = db.r.Encode(buf, data)
		if err != nil {
			return err
		}

		return bucket.Put([]byte(key), buf)
	})
	return err
}

func (db *boltDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	err := db.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(table))
		if err != nil {
			return err
		}

		buf := db.bufPool.Get()
		defer func() {
			db.bufPool.Put(buf)
		}()

		buf, err = db.r.Encode(buf, values)
		if err != nil {
			return err
		}

		return bucket.Put([]byte(key), buf)
	})
	return err
}

func (db *boltDB) Delete(ctx context.Context, table string, key string) error {
	err := db.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(table))
		if bucket == nil {
			return nil
		}

		err := bucket.Delete([]byte(key))
		if err != nil {
			return err
		}

		if bucket.Stats().KeyN == 0 {
			_ = tx.DeleteBucket([]byte(table))
		}
		return nil
	})
	return err
}

func init() {
	ycsb.RegisterDBCreator("boltdb", boltCreator{})
}

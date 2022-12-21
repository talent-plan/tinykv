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

//go:build foundationdb

package foundationdb

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const (
	fdbClusterFile = "fdb.cluster"
	fdbDatabase    = "fdb.dbname"
	fdbAPIVersion  = "fdb.apiversion"
)

type fDB struct {
	db      fdb.Database
	r       *util.RowCodec
	bufPool *util.BufPool
}

func createDB(p *properties.Properties) (ycsb.DB, error) {
	clusterFile := p.GetString(fdbClusterFile, "")
	database := p.GetString(fdbDatabase, "DB")
	apiVersion := p.GetInt(fdbAPIVersion, 510)

	fdb.MustAPIVersion(apiVersion)

	db, err := fdb.Open(clusterFile, []byte(database))
	if err != nil {
		return nil, err
	}

	bufPool := util.NewBufPool()

	return &fDB{
		db:      db,
		r:       util.NewRowCodec(p),
		bufPool: bufPool,
	}, nil
}

func (db *fDB) Close() error {
	return nil
}

func (db *fDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *fDB) CleanupThread(ctx context.Context) {
}

func (db *fDB) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

func (db *fDB) getEndRowKey(table string) []byte {
	// ';' is ':' + 1 in the ASCII
	return util.Slice(fmt.Sprintf("%s;", table))
}

func (db *fDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	rowKey := db.getRowKey(table, key)
	row, err := db.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		f := tr.Get(fdb.Key(rowKey))
		return f.Get()
	})

	if err != nil {
		return nil, err
	} else if row == nil {
		return nil, nil
	}

	return db.r.Decode(row.([]byte), fields)
}

func (db *fDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	rowKey := db.getRowKey(table, startKey)
	res, err := db.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		r := fdb.KeyRange{
			Begin: fdb.Key(rowKey),
			End:   fdb.Key(db.getEndRowKey(table)),
		}
		ri := tr.GetRange(r, fdb.RangeOptions{Limit: count}).Iterator()
		res := make([]map[string][]byte, 0, count)
		for ri.Advance() {
			kv, err := ri.Get()
			if err != nil {
				return nil, err
			}

			if kv.Value == nil {
				res = append(res, nil)
			} else {
				v, err := db.r.Decode(kv.Value, fields)
				if err != nil {
					return nil, err
				}
				res = append(res, v)
			}

		}

		return res, nil
	})
	if err != nil {
		return nil, err
	}
	return res.([]map[string][]byte), nil
}

func (db *fDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	rowKey := db.getRowKey(table, key)
	_, err := db.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		f := tr.Get(fdb.Key(rowKey))
		row, err := f.Get()
		if err != nil {
			return nil, err
		} else if row == nil {
			return nil, nil
		}

		data, err := db.r.Decode(row, nil)
		if err != nil {
			return nil, err
		}

		for field, value := range values {
			data[field] = value
		}

		buf := db.bufPool.Get()
		defer db.bufPool.Put(buf)

		buf, err = db.r.Encode(buf, data)
		if err != nil {
			return nil, err
		}

		tr.Set(fdb.Key(rowKey), buf)
		return
	})

	return err
}

func (db *fDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	// Simulate TiDB data
	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	buf, err := db.r.Encode(buf, values)
	if err != nil {
		return err
	}

	rowKey := db.getRowKey(table, key)
	_, err = db.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.Set(fdb.Key(rowKey), buf)
		return
	})
	return err
}

func (db *fDB) Delete(ctx context.Context, table string, key string) error {
	rowKey := db.getRowKey(table, key)
	_, err := db.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.Clear(fdb.Key(rowKey))
		return
	})
	return err
}

type fdbCreator struct {
}

func (c fdbCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	return createDB(p)
}

func init() {
	ycsb.RegisterDBCreator("fdb", fdbCreator{})
	ycsb.RegisterDBCreator("foundationdb", fdbCreator{})
}

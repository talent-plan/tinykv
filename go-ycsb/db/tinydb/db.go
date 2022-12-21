package tinydb

import (
	"context"
	"fmt"

	"github.com/magiconair/properties"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/server"
	standalone_storage "github.com/pingcap-incubator/tinykv/kv/storage/standalone_storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type tinydbCreator struct {
}

type tinyDB struct {
	p  *properties.Properties
	s  *standalone_storage.StandAloneStorage
	db *server.Server

	r       *util.RowCodec
	bufPool *util.BufPool
}

func (c tinydbCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	fmt.Print("1\n")
	conf := config.NewTestConfig()
	s := standalone_storage.NewStandAloneStorage(conf)
	s.Start()
	server := server.NewServer(s)

	return &tinyDB{
		p:       p,
		s:       s,
		db:      server,
		r:       util.NewRowCodec(p),
		bufPool: util.NewBufPool(),
	}, nil
}

func (db *tinyDB) Close() error {
	return db.s.Stop()
}

func (db *tinyDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *tinyDB) CleanupThread(_ context.Context) {
}

func (db *tinyDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	req := &kvrpcpb.RawGetRequest{
		Key: []byte(key),
		Cf:  table,
	}
	resp, _ := db.db.RawGet(nil, req)
	value := resp.Value
	return db.r.Decode(value, fields)
}

func (db *tinyDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	res := make([]map[string][]byte, count)
	req := &kvrpcpb.RawScanRequest{
		StartKey: []byte(startKey),
		Limit:    uint32(count),
		Cf:       table,
	}
	resp, _ := db.db.RawScan(nil, req)
	kvs := resp.Kvs
	for i := 0; i < count; i++ {
		value := kvs[i].Value
		m, err := db.r.Decode(value, fields)
		if err != nil {
			return nil, err
		}
		res[i] = m
	}
	return res, nil
}

func (db *tinyDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	getReq := &kvrpcpb.RawGetRequest{
		Key: []byte(key),
		Cf:  table,
	}
	resp, err := db.db.RawGet(nil, getReq)
	if err != nil {
		return err
	}
	value := resp.Value
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
	putReq := &kvrpcpb.RawPutRequest{
		Key:   []byte(key),
		Value: buf,
		Cf:    table,
	}

	_, err = db.db.RawPut(nil, putReq)
	if err != nil {
		return err
	}
	return nil
}

func (db *tinyDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	buf := db.bufPool.Get()
	defer func() {
		db.bufPool.Put(buf)
	}()

	buf, err := db.r.Encode(buf, values)
	if err != nil {
		return err
	}
	putReq := &kvrpcpb.RawPutRequest{
		Key:   []byte(key),
		Value: buf,
		Cf:    table,
	}

	_, err = db.db.RawPut(nil, putReq)
	if err != nil {
		return err
	}
	return nil
}

func (db *tinyDB) Delete(ctx context.Context, table string, key string) error {
	req := &kvrpcpb.RawDeleteRequest{
		Key: []byte(key),
		Cf:  table,
	}
	_, err := db.db.RawDelete(nil, req)

	return err

}

func init() {
	ycsb.RegisterDBCreator("tinydb", tinydbCreator{})
}

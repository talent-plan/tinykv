package minio

import (
	"bytes"
	"context"
	"io/ioutil"

	"github.com/magiconair/properties"
	"github.com/minio/minio-go"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const (
	minioAccessKey = "minio.access-key"
	minioSecretKey = "minio.secret-key"
	minioEndpoint  = "minio.endpoint"
	minioSecure    = "minio.secure"
)

type minioCreator struct{}

func (c minioCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	accessKeyID := p.GetString(minioAccessKey, "minio")
	secretAccessKey := p.GetString(minioSecretKey, "myminio")
	endpoint := p.GetString(minioEndpoint, "http://127.0.0.1:9000")
	secure := p.GetBool(minioSecure, false)
	client, err := minio.New(endpoint, accessKeyID, secretAccessKey, secure)
	if err != nil {
		return nil, err
	}
	return &minioDB{
		db: client,
	}, nil
}

type minioDB struct {
	db *minio.Client
}

// Close closes the database layer.
func (db *minioDB) Close() error {
	return nil
}

// InitThread initializes the state associated to the goroutine worker.
// The Returned context will be passed to the following usage.
func (db *minioDB) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return ctx
}

// CleanupThread cleans up the state when the worker finished.
func (db *minioDB) CleanupThread(ctx context.Context) {
}

// Read reads a record from the database and returns a map of each field/value pair.
// table: The name of the table.
// key: The record key of the record to read.
// fields: The list of fields to read, nil|empty for reading all.
func (db *minioDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	obj, err := db.db.GetObjectWithContext(ctx, table, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	bs, err := ioutil.ReadAll(obj)
	if err != nil {
		return nil, err
	}
	return map[string][]byte{"field0": bs}, nil
}

// Scan scans records from the database.
// table: The name of the table.
// startKey: The first record key to read.
// count: The number of records to read.
// fields: The list of fields to read, nil|empty for reading all.
func (db *minioDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	res := make([]map[string][]byte, count)
	done := make(chan struct{})
	defer close(done)
	ch := db.db.ListObjectsV2(table, startKey, true, done)

	for i := 0; i < count; i++ {
		obj, ok := <-ch
		if !ok {
			break
		}
		res[i] = map[string][]byte{obj.Key: nil}
	}
	return res, nil
}

// Update updates a record in the database. Any field/value pairs will be written into the
// database or overwritten the existing values with the same field name.
// table: The name of the table.
// key: The record key of the record to update.
// values: A map of field/value pairs to update in the record.
func (db *minioDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	var bs []byte
	for _, v := range values {
		bs = v
		break
	}
	reader := bytes.NewBuffer(bs)
	size := int64(len(bs))
	_, err := db.db.PutObjectWithContext(ctx, table, key, reader, size, minio.PutObjectOptions{})
	return err
}

// Insert inserts a record in the database. Any field/value pairs will be written into the
// database.
// table: The name of the table.
// key: The record key of the record to insert.
// values: A map of field/value pairs to insert in the record.
func (db *minioDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	return db.Update(ctx, table, key, values)
}

// Delete deletes a record from the database.
// table: The name of the table.
// key: The record key of the record to delete.
func (db *minioDB) Delete(ctx context.Context, table string, key string) error {
	return db.db.RemoveObject(table, key)
}

func init() {
	ycsb.RegisterDBCreator("minio", minioCreator{})
}

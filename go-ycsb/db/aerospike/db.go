package aerospike

import (
	"context"
	"errors"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const (
	asNs   = "aerospike.ns"
	asHost = "aerospike.host"
	asPort = "aerospike.port"
)

type aerospikedb struct {
	client *as.Client
	ns     string
}

// Close closes the database layer.
func (adb *aerospikedb) Close() error {
	adb.client.Close()
	return nil
}

// InitThread initializes the state associated to the goroutine worker.
// The Returned context will be passed to the following usage.
func (adb *aerospikedb) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return ctx
}

// CleanupThread cleans up the state when the worker finished.
func (adb *aerospikedb) CleanupThread(ctx context.Context) {
}

// Read reads a record from the database and returns a map of each field/value pair.
// table: The name of the table.
// key: The record key of the record to read.
// fileds: The list of fields to read, nil|empty for reading all.
func (adb *aerospikedb) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	asKey, err := as.NewKey(adb.ns, table, key)
	record, err := adb.client.Get(nil, asKey)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return map[string][]byte{}, nil
	}
	res := make(map[string][]byte, len(record.Bins))
	var ok bool
	for k, v := range record.Bins {
		res[k], ok = v.([]byte)
		if !ok {
			return nil, errors.New("couldn't convert to byte array")
		}
	}
	return res, nil
}

// Scan scans records from the database.
// table: The name of the table.
// startKey: The first record key to read.
// count: The number of records to read.
// fields: The list of fields to read, nil|empty for reading all.
func (adb *aerospikedb) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	policy := as.NewScanPolicy()
	policy.ConcurrentNodes = true
	recordset, err := adb.client.ScanAll(policy, adb.ns, table)
	if err != nil {
		return nil, err
	}
	filter := make(map[string]bool, len(fields))
	for _, field := range fields {
		filter[field] = true
	}
	scanRes := make([]map[string][]byte, 0)
	var ok bool
	nRead := 0
	for res := range recordset.Results() {
		if res.Err != nil {
			recordset.Close()
			return nil, res.Err
		}
		vals := make(map[string][]byte, len(res.Record.Bins))
		for k, v := range res.Record.Bins {
			if !filter[k] {
				continue
			}
			vals[k], ok = v.([]byte)
			if !ok {
				return nil, errors.New("couldn't convert to byte array")
			}
		}
		scanRes = append(scanRes, vals)
		nRead++
		if nRead == count {
			break
		}
	}
	return scanRes, nil
}

// Update updates a record in the database. Any field/value pairs will be written into the
// database or overwritten the existing values with the same field name.
// table: The name of the table.
// key: The record key of the record to update.
// values: A map of field/value pairs to update in the record.
func (adb *aerospikedb) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	asKey, err := as.NewKey(adb.ns, table, key)
	if err != nil {
		return err
	}
	record, err := adb.client.Get(nil, asKey)
	if err != nil {
		return err
	}
	bins := as.BinMap{}
	var policy *as.WritePolicy
	if record != nil {
		bins = record.Bins
		policy := as.NewWritePolicy(record.Generation, 0)
		policy.GenerationPolicy = as.EXPECT_GEN_EQUAL
	}
	for k, v := range values {
		bins[k] = v
	}
	return adb.client.Put(policy, asKey, bins)
}

// Insert inserts a record in the database. Any field/value pairs will be written into the
// database.
// table: The name of the table.
// key: The record key of the record to insert.
// values: A map of field/value pairs to insert in the record.
func (adb *aerospikedb) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	asKey, err := as.NewKey(adb.ns, table, key)
	if err != nil {
		return err
	}
	bins := make([]*as.Bin, len(values))
	i := 0
	for k, v := range values {
		bins[i] = as.NewBin(k, v)
		i++
	}
	return adb.client.PutBins(nil, asKey, bins...)
}

// Delete deletes a record from the database.
// table: The name of the table.
// key: The record key of the record to delete.
func (adb *aerospikedb) Delete(ctx context.Context, table string, key string) error {
	asKey, err := as.NewKey(adb.ns, table, key)
	if err != nil {
		return err
	}
	_, err = adb.client.Delete(nil, asKey)
	return err
}

type aerospikeCreator struct{}

func (a aerospikeCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	adb := &aerospikedb{}
	adb.ns = p.GetString(asNs, "test")
	var err error
	adb.client, err = as.NewClient(p.GetString(asHost, "localhost"), p.GetInt(asPort, 3000))
	return adb, err
}

func init() {
	ycsb.RegisterDBCreator("aerospike", aerospikeCreator{})
}

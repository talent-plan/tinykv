package etcd

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/magiconair/properties"
	"go.etcd.io/etcd/client/pkg/v3/transport"

	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// properties
const (
	etcdEndpoints   = "etcd.endpoints"
	etcdDialTimeout = "etcd.dial_timeout"
	etcdCertFile    = "etcd.cert_file"
	etcdKeyFile     = "etcd.key_file"
	etcdCaFile      = "etcd.cacert_file"
)

type etcdCreator struct{}

type etcdDB struct {
	p      *properties.Properties
	client *clientv3.Client
}

func init() {
	ycsb.RegisterDBCreator("etcd", etcdCreator{})
}

func (c etcdCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	cfg, err := getClientConfig(p)
	if err != nil {
		return nil, err
	}

	client, err := clientv3.New(*cfg)
	if err != nil {
		return nil, err
	}

	return &etcdDB{
		p:      p,
		client: client,
	}, nil
}

func getClientConfig(p *properties.Properties) (*clientv3.Config, error) {
	endpoints := p.GetString(etcdEndpoints, "localhost:2379")
	dialTimeout := p.GetDuration(etcdDialTimeout, 2*time.Second)

	var tlsConfig *tls.Config
	if strings.Contains(endpoints, "https") {
		tlsInfo := transport.TLSInfo{
			CertFile:      p.MustGetString(etcdCertFile),
			KeyFile:       p.MustGetString(etcdKeyFile),
			TrustedCAFile: p.MustGetString(etcdCaFile),
		}
		c, err := tlsInfo.ClientConfig()
		if err != nil {
			return nil, err
		}
		tlsConfig = c
	}

	return &clientv3.Config{
		Endpoints:   strings.Split(endpoints, ","),
		DialTimeout: dialTimeout,
		TLS:         tlsConfig,
	}, nil
}

func (db *etcdDB) Close() error {
	return db.client.Close()
}

func (db *etcdDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *etcdDB) CleanupThread(_ context.Context) {
}

func getRowKey(table string, key string) string {
	return fmt.Sprintf("%s:%s", table, key)
}

func (db *etcdDB) Read(ctx context.Context, table string, key string, _ []string) (map[string][]byte, error) {
	rkey := getRowKey(table, key)
	value, err := db.client.Get(ctx, rkey)
	if err != nil {
		return nil, err
	}

	if value.Count == 0 {
		return nil, fmt.Errorf("could not find value for key [%s]", rkey)
	}

	var r map[string][]byte
	err = json.NewDecoder(bytes.NewReader(value.Kvs[0].Value)).Decode(&r)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (db *etcdDB) Scan(ctx context.Context, table string, startKey string, count int, _ []string) ([]map[string][]byte, error) {
	res := make([]map[string][]byte, count)
	rkey := getRowKey(table, startKey)
	values, err := db.client.Get(ctx, rkey, clientv3.WithFromKey(), clientv3.WithLimit(int64(count)))
	if err != nil {
		return nil, err
	}

	if values.Count != int64(count) {
		return nil, fmt.Errorf("unexpected number of result for key [%s], expected %d but was %d", rkey, count, values.Count)
	}

	for _, v := range values.Kvs {
		var r map[string][]byte
		err = json.NewDecoder(bytes.NewReader(v.Value)).Decode(&r)
		if err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}

func (db *etcdDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	rkey := getRowKey(table, key)
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}
	_, err = db.client.Put(ctx, rkey, string(data))
	if err != nil {
		return err
	}

	return nil
}

func (db *etcdDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	return db.Update(ctx, table, key, values)
}

func (db *etcdDB) Delete(ctx context.Context, table string, key string) error {
	_, err := db.client.Delete(ctx, getRowKey(table, key))
	if err != nil {
		return err
	}
	return nil
}

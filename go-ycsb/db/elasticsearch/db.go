package elastic

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"net"
	"net/http"
	"runtime"
	"strings"
	"time"
)

const (
	elasticUrl                                 = "es.hosts.list"
	elasticUrlDefault                          = "http://127.0.0.1:9200"
	elasticInsecureSSLProp                     = "es.insecure.ssl"
	elasticInsecureSSLPropDefault              = false
	elasticExitOnIndexCreateFailureProp        = "es.exit.on.index.create.fail"
	elasticExitOnIndexCreateFailurePropDefault = false
	elasticShardCountProp                      = "es.number_of_shards"
	elasticShardCountPropDefault               = 1
	elasticReplicaCountProp                    = "es.number_of_replicas"
	elasticReplicaCountPropDefault             = 0
	elasticUsername                            = "es.username"
	elasticUsernameDefault                     = "elastic"
	elasticPassword                            = "es.password"
	elasticPasswordPropDefault                 = ""
	elasticFlushInterval                       = "es.flush_interval"
	bulkIndexerNumberOfWorkers                 = "es.bulk.num_workers"
	elasticMaxRetriesProp                      = "es.max_retires"
	elasticMaxRetriesPropDefault               = 10
	bulkIndexerFlushBytesProp                  = "es.bulk.flush_bytes"
	bulkIndexerFlushBytesDefault               = 5e+6
	bulkIndexerFlushIntervalSecondsProp        = "es.bulk.flush_interval_secs"
	bulkIndexerFlushIntervalSecondsPropDefault = 30
	elasticIndexNameDefault                    = "ycsb"
	elasticIndexName                           = "es.index"
)

type elastic struct {
	cli       *elasticsearch.Client
	bi        esutil.BulkIndexer
	indexName string
	verbose   bool
}

func (m *elastic) Close() error {
	return nil
}

func (m *elastic) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return ctx
}

func (m *elastic) CleanupThread(ctx context.Context) {
	m.bi.Close(context.Background())
}

// Read a document.
func (m *elastic) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	res, err := m.cli.Get(m.indexName, key)
	if err != nil {
		if m.verbose {
			fmt.Println("Cannot read document %d: %s", key, err)
		}
		return nil, err
	}
	var r map[string][]byte
	json.NewDecoder(res.Body).Decode(&r)
	return r, nil
}

// Scan documents.
func (m *elastic) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")

}

// Insert a document.
func (m *elastic) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	data, err := json.Marshal(values)
	if err != nil {
		if m.verbose {
			fmt.Println("Cannot encode document %d: %s", key, err)
		}
		return err
	}
	// Add an item to the BulkIndexer
	err = m.bi.Add(
		context.Background(),
		esutil.BulkIndexerItem{
			// Action field configures the operation to perform (index, create, delete, update)
			Action: "index",

			// DocumentID is the (optional) document ID
			DocumentID: key,

			// Body is an `io.Reader` with the payload
			Body: bytes.NewReader(data),

			// OnSuccess is called for each successful operation
			OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
			},
			// OnFailure is called for each failed operation
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				if err != nil {
					fmt.Printf("ERROR BULK INSERT: %s", err)
				} else {
					fmt.Printf("ERROR BULK INSERT: %s: %s", res.Error.Type, res.Error.Reason)
				}
			},
		},
	)
	if err != nil {
		if m.verbose {
			fmt.Println("Unexpected error while bulk inserting: %s", err)
		}
		return err
	}
	return nil
}

// Update a document.
func (m *elastic) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	data, err := json.Marshal(values)
	if err != nil {
		if m.verbose {
			fmt.Println("Cannot encode document %d: %s", key, err)
		}
		return err
	}
	// Add an item to the BulkIndexer
	err = m.bi.Add(
		context.Background(),
		esutil.BulkIndexerItem{
			// Action field configures the operation to perform (index, create, delete, update)
			Action: "update",

			// DocumentID is the (optional) document ID
			DocumentID: key,

			// Body is an `io.Reader` with the payload
			Body: bytes.NewReader(data),

			// OnSuccess is called for each successful operation
			OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
			},
			// OnFailure is called for each failed operation
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				if err != nil {
					fmt.Printf("ERROR BULK UPDATING: %s", err)
				} else {
					fmt.Printf("ERROR BULK UPDATING: %s: %s", res.Error.Type, res.Error.Reason)
				}
			},
		},
	)
	if err != nil {
		if m.verbose {
			fmt.Println("Unexpected error while bulk updating: %s", err)
		}
		return err
	}
	return nil
}

// Delete a document.
func (m *elastic) Delete(ctx context.Context, table string, key string) error {
	// Add an delete to the BulkIndexer
	err := m.bi.Add(
		context.Background(),
		esutil.BulkIndexerItem{
			// Action field configures the operation to perform (index, create, delete, update)
			Action: "delete",

			// DocumentID is the (optional) document ID
			DocumentID: key,

			// OnSuccess is called for each successful operation
			OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
			},
			// OnFailure is called for each failed operation
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				if err != nil {
					fmt.Printf("ERROR BULK UPDATING: %s", err)
				} else {
					fmt.Printf("ERROR BULK UPDATING: %s: %s", res.Error.Type, res.Error.Reason)
				}
			},
		},
	)
	if err != nil {
		if m.verbose {
			fmt.Println("Unexpected error while bulk deleting: %s", err)
		}
		return err
	}
	return nil
}

type elasticCreator struct {
}

func (c elasticCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	defaultNumCpus := runtime.NumCPU()
	bulkIndexerRefresh := "false"
	batchSize := p.GetInt(prop.BatchSize, prop.DefaultBatchSize)
	if batchSize <= 1 {
		bulkIndexerRefresh = "wait_for"
		fmt.Printf("Bulk API is disable given the property `%s`=1. For optimal indexing speed please increase this value property\n", prop.BatchSize)
	}
	bulkIndexerNumCpus := p.GetInt(bulkIndexerNumberOfWorkers, defaultNumCpus)
	elasticMaxRetries := p.GetInt(elasticMaxRetriesProp, elasticMaxRetriesPropDefault)
	bulkIndexerFlushBytes := p.GetInt(bulkIndexerFlushBytesProp, bulkIndexerFlushBytesDefault)
	flushIntervalSeconds := p.GetInt(bulkIndexerFlushIntervalSecondsProp, bulkIndexerFlushIntervalSecondsPropDefault)
	elasticReplicaCount := p.GetInt(elasticReplicaCountProp, elasticReplicaCountPropDefault)
	elasticShardCount := p.GetInt(elasticShardCountProp, elasticShardCountPropDefault)

	addressesS := p.GetString(elasticUrl, elasticUrlDefault)
	insecureSSL := p.GetBool(elasticInsecureSSLProp, elasticInsecureSSLPropDefault)
	failOnCreate := p.GetBool(elasticExitOnIndexCreateFailureProp, elasticExitOnIndexCreateFailurePropDefault)
	esUser := p.GetString(elasticUsername, elasticUsernameDefault)
	esPass := p.GetString(elasticPassword, elasticPasswordPropDefault)
	command, _ := p.Get(prop.Command)
	verbose := p.GetBool(prop.Verbose, prop.VerboseDefault)
	iname := p.GetString(elasticIndexName, elasticIndexNameDefault)
	addresses := strings.Split(addressesS, ",")

	retryBackoff := backoff.NewExponentialBackOff()
	//
	//// Get the SystemCertPool, continue with an empty pool on error
	//rootCAs, _ := x509.SystemCertPool()
	//if rootCAs == nil {
	//	rootCAs = x509.NewCertPool()
	//}

	cfg := elasticsearch.Config{
		Addresses: addresses,
		// Retry on 429 TooManyRequests statuses
		RetryOnStatus: []int{502, 503, 504, 429},

		// Configure the backoff function
		RetryBackoff: func(i int) time.Duration {
			if i == 1 {
				retryBackoff.Reset()
			}
			return retryBackoff.NextBackOff()
		},
		MaxRetries: elasticMaxRetries,
		Username:   esUser,
		Password:   esPass,
		// Transport / SSL
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: time.Second,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: insecureSSL,
			},
		},
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		fmt.Println(fmt.Sprintf("Error creating the elastic client: %s", err))
		return nil, err
	}
	fmt.Println("Connected to elastic!")
	if verbose {
		fmt.Println(es.Info())
	}
	// Create the BulkIndexer
	var flushIntervalTime = flushIntervalSeconds * int(time.Second)

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         iname,                            // The default index name
		Client:        es,                               // The Elasticsearch client
		NumWorkers:    bulkIndexerNumCpus,               // The number of worker goroutines
		FlushBytes:    bulkIndexerFlushBytes,            // The flush threshold in bytes
		FlushInterval: time.Duration(flushIntervalTime), // The periodic flush interval
		// If true, Elasticsearch refreshes the affected
		// shards to make this operation visible to search
		// if wait_for then wait for a refresh to make this operation visible to search,
		// if false do nothing with refreshes. Valid values: true, false, wait_for. Default: false.
		Refresh: bulkIndexerRefresh,
	})
	if err != nil {
		fmt.Println("Error creating the elastic indexer: %s", err)
		return nil, err
	}

	if strings.Compare("load", command) == 0 {
		fmt.Println("Ensuring that if the index exists we recreat it")
		// Re-create the index
		var res *esapi.Response
		if res, err = es.Indices.Delete([]string{iname}, es.Indices.Delete.WithIgnoreUnavailable(true)); err != nil || res.IsError() {
			fmt.Println(fmt.Sprintf("Cannot delete index: %s", err))
			return nil, err
		}
		res.Body.Close()

		// Define index mapping.
		mapping := map[string]interface{}{"settings": map[string]interface{}{"index": map[string]interface{}{"number_of_shards": elasticShardCount, "number_of_replicas": elasticReplicaCount}}}
		data, err := json.Marshal(mapping)
		if err != nil {
			if verbose {
				fmt.Println(fmt.Sprintf("Cannot encode index mapping %v: %s", mapping, err))
			}
			return nil, err
		}
		res, err = es.Indices.Create(iname, es.Indices.Create.WithBody(strings.NewReader(string(data))))
		if err != nil && failOnCreate {
			fmt.Println(fmt.Sprintf("Cannot create index: %s", err))
			return nil, err
		}
		if res.IsError() && failOnCreate {
			fmt.Println(fmt.Sprintf("Cannot create index: %s", res))
			return nil, fmt.Errorf("Cannot create index: %s", res)
		}
		res.Body.Close()
	}

	m := &elastic{
		cli:       es,
		bi:        bi,
		indexName: iname,
		verbose:   verbose,
	}
	return m, nil
}

func init() {
	ycsb.RegisterDBCreator("elastic", elasticCreator{})
}

package redis

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	goredis "github.com/go-redis/redis/v9"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const HASH_DATATYPE string = "hash"
const STRING_DATATYPE string = "string"
const JSON_DATATYPE string = "json"
const JSON_SET string = "JSON.SET"
const JSON_GET string = "JSON.GET"
const HSET string = "HSET"
const HMGET string = "HMGET"

type redisClient interface {
	Get(ctx context.Context, key string) *goredis.StringCmd
	Do(ctx context.Context, args ...interface{}) *goredis.Cmd
	Pipeline() goredis.Pipeliner
	Scan(ctx context.Context, cursor uint64, match string, count int64) *goredis.ScanCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *goredis.StatusCmd
	Del(ctx context.Context, keys ...string) *goredis.IntCmd
	FlushDB(ctx context.Context) *goredis.StatusCmd
	Close() error
}

type redis struct {
	client   redisClient
	mode     string
	datatype string
}

func (r *redis) Close() error {
	return r.client.Close()
}

func (r *redis) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (r *redis) CleanupThread(_ context.Context) {
}

func (r *redis) Read(ctx context.Context, table string, key string, fields []string) (data map[string][]byte, err error) {
	data = make(map[string][]byte, len(fields))
	err = nil
	switch r.datatype {
	case JSON_DATATYPE:
		cmds := make([]*goredis.Cmd, len(fields))
		pipe := r.client.Pipeline()
		for pos, fieldName := range fields {
			cmds[pos] = pipe.Do(ctx, JSON_GET, getKeyName(table, key), getFieldJsonPath(fieldName))
		}
		_, err = pipe.Exec(ctx)
		if err != nil {
			return
		}
		var s string = ""
		for pos, fieldName := range fields {
			s, err = cmds[pos].Text()
			if err != nil {
				return
			}
			data[fieldName] = []byte(s)
		}
	case HASH_DATATYPE:
		args := make([]interface{}, 0, len(fields)+2)
		args = append(args, HMGET, getKeyName(table, key))
		for _, fieldName := range fields {
			args = append(args, fieldName)
		}
		sliceReply, errI := r.client.Do(ctx, args...).StringSlice()
		if errI != nil {
			return
		}
		for pos, slicePos := range sliceReply {
			data[fields[pos]] = []byte(slicePos)
		}
	case STRING_DATATYPE:
		fallthrough
	default:
		{
			var res string = ""
			res, err = r.client.Get(ctx, getKeyName(table, key)).Result()
			if err != nil {
				return
			}
			err = json.Unmarshal([]byte(res), &data)
			return
		}
	}
	return

}

func (r *redis) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")
}

func (r *redis) Update(ctx context.Context, table string, key string, values map[string][]byte) (err error) {
	err = nil
	switch r.datatype {
	case JSON_DATATYPE:
		cmds := make([]*goredis.Cmd, 0, len(values))
		pipe := r.client.Pipeline()
		for fieldName, bytes := range values {
			cmd := pipe.Do(ctx, JSON_SET, getKeyName(table, key), getFieldJsonPath(fieldName), jsonEscape(bytes))
			cmds = append(cmds, cmd)
		}
		_, err = pipe.Exec(ctx)
		if err != nil {
			return
		}
		for _, cmd := range cmds {
			err = cmd.Err()
			if err != nil {
				return
			}
		}
	case HASH_DATATYPE:
		args := make([]interface{}, 0, 2*len(values)+2)
		args = append(args, HSET, getKeyName(table, key))
		for fieldName, bytes := range values {
			args = append(args, fieldName, string(bytes))
		}
		err = r.client.Do(ctx, args...).Err()
	case STRING_DATATYPE:
		fallthrough
	default:
		{
			var initialEncodedJson string = ""
			initialEncodedJson, err = r.client.Get(ctx, getKeyName(table, key)).Result()
			if err != nil {
				return
			}
			var encodedJson = make([]byte, 0)
			err, encodedJson = mergeEncodedJsonWithMap(initialEncodedJson, values)
			if err != nil {
				return
			}
			return r.client.Set(ctx, getKeyName(table, key), string(encodedJson), 0).Err()
		}
	}
	return
}

func mergeEncodedJsonWithMap(stringReply string, values map[string][]byte) (err error, data []byte) {
	curVal := map[string][]byte{}
	err = json.Unmarshal([]byte(stringReply), &curVal)
	if err != nil {
		return
	}
	for k, v := range values {
		curVal[k] = v
	}
	data, err = json.Marshal(curVal)
	return
}

func jsonEscape(bytes []byte) string {
	return fmt.Sprintf("\"%s\"", string(bytes))
}

func getFieldJsonPath(fieldName string) string {
	return fmt.Sprintf("$.%s", fieldName)
}

func getKeyName(table string, key string) string {
	return table + "/" + key
}

func (r *redis) Insert(ctx context.Context, table string, key string, values map[string][]byte) (err error) {
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}
	switch r.datatype {
	case JSON_DATATYPE:
		err = r.client.Do(ctx, JSON_SET, getKeyName(table, key), ".", string(data)).Err()
	case HASH_DATATYPE:
		args := make([]interface{}, 0, 2*len(values)+2)
		args = append(args, HSET, getKeyName(table, key))
		for fieldName, bytes := range values {
			args = append(args, fieldName, string(bytes))
		}
		err = r.client.Do(ctx, args...).Err()
	case STRING_DATATYPE:
		fallthrough
	default:
		err = r.client.Set(ctx, getKeyName(table, key), string(data), 0).Err()
	}
	return
}

func (r *redis) Delete(ctx context.Context, table string, key string) error {
	return r.client.Del(ctx, getKeyName(table, key)).Err()
}

type redisCreator struct{}

func (r redisCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	rds := &redis{}

	mode := p.GetString(redisMode, redisModeDefault)
	switch mode {
	case "cluster":
		rds.client = goredis.NewClusterClient(getOptionsCluster(p))

		if p.GetBool(prop.DropData, prop.DropDataDefault) {
			err := rds.client.FlushDB(context.Background()).Err()
			if err != nil {
				return nil, err
			}
		}
	case "single":
		fallthrough
	default:
		mode = "single"
		rds.client = goredis.NewClient(getOptionsSingle(p))

		if p.GetBool(prop.DropData, prop.DropDataDefault) {
			err := rds.client.FlushDB(context.Background()).Err()
			if err != nil {
				return nil, err
			}
		}
	}
	rds.mode = mode
	rds.datatype = p.GetString(redisDatatype, redisDatatypeDefault)
	fmt.Println(fmt.Sprintf("Using the redis datatype: %s", rds.datatype))

	return rds, nil
}

const (
	redisMode                  = "redis.mode"
	redisModeDefault           = "single"
	redisDatatype              = "redis.datatype"
	redisDatatypeDefault       = "hash"
	redisNetwork               = "redis.network"
	redisNetworkDefault        = "tcp"
	redisAddr                  = "redis.addr"
	redisAddrDefault           = "localhost:6379"
	redisPassword              = "redis.password"
	redisDB                    = "redis.db"
	redisMaxRedirects          = "redis.max_redirects"
	redisReadOnly              = "redis.read_only"
	redisRouteByLatency        = "redis.route_by_latency"
	redisRouteRandomly         = "redis.route_randomly"
	redisMaxRetries            = "redis.max_retries"
	redisMinRetryBackoff       = "redis.min_retry_backoff"
	redisMaxRetryBackoff       = "redis.max_retry_backoff"
	redisDialTimeout           = "redis.dial_timeout"
	redisReadTimeout           = "redis.read_timeout"
	redisWriteTimeout          = "redis.write_timeout"
	redisPoolSize              = "redis.pool_size"
	redisPoolSizeDefault       = 0
	redisMinIdleConns          = "redis.min_idle_conns"
	redisMaxConnAge            = "redis.max_conn_age"
	redisPoolTimeout           = "redis.pool_timeout"
	redisIdleTimeout           = "redis.idle_timeout"
	redisIdleCheckFreq         = "redis.idle_check_frequency"
	redisTLSCA                 = "redis.tls_ca"
	redisTLSCert               = "redis.tls_cert"
	redisTLSKey                = "redis.tls_key"
	redisTLSInsecureSkipVerify = "redis.tls_insecure_skip_verify"
)

func parseTLS(p *properties.Properties) *tls.Config {
	caPath, _ := p.Get(redisTLSCA)
	certPath, _ := p.Get(redisTLSCert)
	keyPath, _ := p.Get(redisTLSKey)
	insecureSkipVerify := p.GetBool(redisTLSInsecureSkipVerify, false)
	if certPath != "" && keyPath != "" {
		config, err := util.CreateTLSConfig(caPath, certPath, keyPath, insecureSkipVerify)
		if err == nil {
			return config
		}
	}

	return nil
}

func getOptionsSingle(p *properties.Properties) *goredis.Options {
	opts := &goredis.Options{}

	opts.Addr = p.GetString(redisAddr, redisAddrDefault)
	opts.DB = p.GetInt(redisDB, 0)
	opts.Network = p.GetString(redisNetwork, redisNetworkDefault)
	opts.Password, _ = p.Get(redisPassword)
	opts.MaxRetries = p.GetInt(redisMaxRetries, 0)
	opts.MinRetryBackoff = p.GetDuration(redisMinRetryBackoff, time.Millisecond*8)
	opts.MaxRetryBackoff = p.GetDuration(redisMaxRetryBackoff, time.Millisecond*512)
	opts.DialTimeout = p.GetDuration(redisDialTimeout, time.Second*5)
	opts.ReadTimeout = p.GetDuration(redisReadTimeout, time.Second*3)
	opts.WriteTimeout = p.GetDuration(redisWriteTimeout, opts.ReadTimeout)
	opts.PoolSize = p.GetInt(redisPoolSize, redisPoolSizeDefault)
	threadCount := p.MustGetInt("threadcount")
	if opts.PoolSize == 0 {
		opts.PoolSize = threadCount
		fmt.Println(fmt.Sprintf("Setting %s=%d (from <threadcount>) given you haven't specified a value.", redisPoolSize, opts.PoolSize))
	}
	opts.MinIdleConns = p.GetInt(redisMinIdleConns, 0)
	opts.MaxConnAge = p.GetDuration(redisMaxConnAge, 0)
	opts.PoolTimeout = p.GetDuration(redisPoolTimeout, time.Second+opts.ReadTimeout)
	opts.IdleTimeout = p.GetDuration(redisIdleTimeout, time.Minute*5)
	opts.IdleCheckFrequency = p.GetDuration(redisIdleCheckFreq, time.Minute)
	opts.TLSConfig = parseTLS(p)

	return opts
}

func getOptionsCluster(p *properties.Properties) *goredis.ClusterOptions {
	opts := &goredis.ClusterOptions{}

	addresses, _ := p.Get(redisAddr)
	opts.Addrs = strings.Split(addresses, ";")
	opts.MaxRedirects = p.GetInt(redisMaxRedirects, 0)
	opts.ReadOnly = p.GetBool(redisReadOnly, false)
	opts.RouteByLatency = p.GetBool(redisRouteByLatency, false)
	opts.RouteRandomly = p.GetBool(redisRouteRandomly, false)
	opts.Password, _ = p.Get(redisPassword)
	opts.MaxRetries = p.GetInt(redisMaxRetries, 0)
	opts.MinRetryBackoff = p.GetDuration(redisMinRetryBackoff, time.Millisecond*8)
	opts.MaxRetryBackoff = p.GetDuration(redisMaxRetryBackoff, time.Millisecond*512)
	opts.DialTimeout = p.GetDuration(redisDialTimeout, time.Second*5)
	opts.ReadTimeout = p.GetDuration(redisReadTimeout, time.Second*3)
	opts.WriteTimeout = p.GetDuration(redisWriteTimeout, opts.ReadTimeout)
	opts.PoolSize = p.GetInt(redisPoolSize, redisPoolSizeDefault)
	threadCount := p.MustGetInt("threadcount")
	if opts.PoolSize == 0 {
		opts.PoolSize = threadCount
		fmt.Println(fmt.Sprintf("Setting %s=%d (from <threadcount>) given you haven't specified a value.", redisPoolSize, opts.PoolSize))
	}
	opts.MinIdleConns = p.GetInt(redisMinIdleConns, 0)
	opts.MaxConnAge = p.GetDuration(redisMaxConnAge, 0)
	opts.PoolTimeout = p.GetDuration(redisPoolTimeout, time.Second+opts.ReadTimeout)
	opts.IdleTimeout = p.GetDuration(redisIdleTimeout, time.Minute*5)
	opts.IdleCheckFrequency = p.GetDuration(redisIdleCheckFreq, time.Minute)

	opts.TLSConfig = parseTLS(p)

	return opts
}

func init() {
	ycsb.RegisterDBCreator("redis", redisCreator{})
}

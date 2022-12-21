# go-ycsb

go-ycsb is a Go port of [YCSB](https://github.com/brianfrankcooper/YCSB). It fully supports all YCSB generators and the Core workload so we can do the basic CRUD benchmarks with Go.

## Why another Go YCSB?

+ We want to build a standard benchmark tool in Go.
+ We are not familiar with Java.

## Getting Started

### Download

https://github.com/pingcap/go-ycsb/releases/latest

**Linux**
```
wget -c https://github.com/pingcap/go-ycsb/releases/latest/download/go-ycsb-linux-amd64.tar.gz -O - | tar -xz

# give it a try
./go-ycsb --help
```

**OSX**
```
wget -c https://github.com/pingcap/go-ycsb/releases/latest/download/go-ycsb-darwin-amd64.tar.gz -O - | tar -xz

# give it a try
./go-ycsb --help
```

### Building from source

```bash
git clone https://github.com/pingcap/go-ycsb.git
cd go-ycsb
make

# give it a try
./bin/go-ycsb  --help
```

Notice:

+ Minimum supported go version is 1.16.
+ To use FoundationDB, you must install [client](https://www.foundationdb.org/download/) library at first, now the supported version is 6.2.11.
+ To use RocksDB, you must follow [INSTALL](https://github.com/facebook/rocksdb/blob/master/INSTALL.md) to install RocksDB at first.

## Usage

Mostly, we can start from the official document [Running-a-Workload](https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload).

### Shell

```basic
./bin/go-ycsb shell basic
» help
YCSB shell command

Usage:
  shell [command]

Available Commands:
  delete      Delete a record
  help        Help about any command
  insert      Insert a record
  read        Read a record
  scan        Scan starting at key
  table       Get or [set] the name of the table
  update      Update a record
```

### Load

```bash
./bin/go-ycsb load basic -P workloads/workloada
```

### Run

```bash
./bin/go-ycsb run basic -P workloads/workloada
```

## Supported Database

- MySQL / TiDB
- TiKV
- FoundationDB
- Aerospike
- Badger
- Cassandra / ScyllaDB
- Pegasus
- PostgreSQL / CockroachDB / AlloyDB / Yugabyte
- RocksDB
- Spanner
- Sqlite
- MongoDB
- Redis and Redis Cluster
- BoltDB
- etcd
- DynamoDB

## Output configuration

|field|default value|description|
|-|-|-|
|measurementtype|"histogram"|The mechanism for recording measurements, one of `histogram`, `raw` or `csv`|
|measurement.output_file|""|File to write output to, default writes to stdout|

## Database Configuration

You can pass the database configurations through `-p field=value` in the command line directly.

Common configurations:

|field|default value|description|
|-|-|-|
|dropdata|false|Whether to remove all data before test|
|verbose|false|Output the execution query|
|debug.pprof|":6060"|Go debug profile address|

### MySQL & TiDB

|field|default value|description|
|-|-|-|
|mysql.host|"127.0.0.1"|MySQL Host|
|mysql.port|3306|MySQL Port|
|mysql.user|"root"|MySQL User|
|mysql.password||MySQL Password|
|mysql.db|"test"|MySQL Database|
|tidb.cluster_index|true|Whether to use cluster index, for TiDB only|
|tidb.instances|""|Comma-seperated address list of tidb instances (eg: `tidb-0:4000,tidb-1:4000`)|


### TiKV

|field|default value|description|
|-|-|-|
|tikv.pd|"127.0.0.1:2379"|PD endpoints, seperated by comma|
|tikv.type|"raw"|TiKV mode, "raw", "txn", or "coprocessor"|
|tikv.conncount|128|gRPC connection count|
|tikv.batchsize|128|Request batch size|
|tikv.async_commit|true|Enalbe async commit or not|
|tikv.one_pc|true|Enable one phase or not|
|tikv.apiversion|"V1"|[api-version](https://docs.pingcap.com/tidb/stable/tikv-configuration-file#api-version-new-in-v610) of tikv server, "V1" or "V2"|

### FoundationDB

|field|default value|description|
|-|-|-|
|fdb.cluster|""|The cluster file used for FoundationDB, if not set, will use the [default](https://apple.github.io/foundationdb/administration.html#default-cluster-file)|
|fdb.dbname|"DB"|The cluster database name|
|fdb.apiversion|510|API version, now only 5.1 is supported|

### PostgreSQL & CockroachDB & AlloyDB & Yugabyte

|field|default value|description|
|-|-|-|
|pg.host|"127.0.0.1"|PostgreSQL Host|
|pg.port|5432|PostgreSQL Port|
|pg.user|"root"|PostgreSQL User|
|pg.password||PostgreSQL Password|
|pg.db|"test"|PostgreSQL Database|
|pg.sslmode|"disable|PostgreSQL ssl mode|

### Aerospike

|field|default value|description|
|-|-|-|
|aerospike.host|"localhost"|The port of the Aerospike service|
|aerospike.port|3000|The port of the Aerospike service|
|aerospike.ns|"test"|The namespace to use|

### Badger

|field|default value|description|
|-|-|-|
|badger.dir|"/tmp/badger"|The directory to save data|
|badger.valuedir|"/tmp/badger"|The directory to save value, if not set, use badger.dir|
|badger.sync_writes|false|Sync all writes to disk|
|badger.num_versions_to_keep|1|How many versions to keep per key|
|badger.max_table_size|64MB|Each table (or file) is at most this size|
|badger.level_size_multiplier|10|Equals SizeOf(Li+1)/SizeOf(Li)|
|badger.max_levels|7|Maximum number of levels of compaction|
|badger.value_threshold|32|If value size >= this threshold, only store value offsets in tree|
|badger.num_memtables|5|Maximum number of tables to keep in memory, before stalling|
|badger.num_level0_tables|5|Maximum number of Level 0 tables before we start compacting|
|badger.num_level0_tables_stall|10|If we hit this number of Level 0 tables, we will stall until L0 is compacted away|
|badger.level_one_size|256MB|Maximum total size for L1|
|badger.value_log_file_size|1GB|Size of single value log file|
|badger.value_log_max_entries|1000000|Max number of entries a value log file can hold (approximately). A value log file would be determined by the smaller of its file size and max entries|
|badger.num_compactors|3|Number of compaction workers to run concurrently|
|badger.do_not_compact|false|Stops LSM tree from compactions|
|badger.table_loading_mode|options.LoadToRAM|How should LSM tree be accessed|
|badger.value_log_loading_mode|options.MemoryMap|How should value log be accessed|

### RocksDB

|field|default value|description|
|-|-|-|
|rocksdb.dir|"/tmp/rocksdb"|The directory to save data|
|rocksdb.allow_concurrent_memtable_writes|true|Sets whether to allow concurrent memtable writes|
|rocksdb.allow_mmap_reads|false|Enable/Disable mmap reads for reading sst tables|
|rocksdb.allow_mmap_writes|false|Enable/Disable mmap writes for writing sst tables|
|rocksdb.arena_block_size|0(write_buffer_size / 8)|Sets the size of one block in arena memory allocation|
|rocksdb.db_write_buffer_size|0(disable)|Sets the amount of data to build up in memtables across all column families before writing to disk|
|rocksdb.hard_pending_compaction_bytes_limit|256GB|Sets the bytes threshold at which all writes are stopped if estimated bytes needed to be compaction exceed this threshold|
|rocksdb.level0_file_num_compaction_trigger|4|Sets the number of files to trigger level-0 compaction|
|rocksdb.level0_slowdown_writes_trigger|20|Sets the soft limit on number of level-0 files|
|rocksdb.level0_stop_writes_trigger|36|Sets the maximum number of level-0 files. We stop writes at this point|
|rocksdb.max_bytes_for_level_base|256MB|Sets the maximum total data size for base level|
|rocksdb.max_bytes_for_level_multiplier|10|Sets the max Bytes for level multiplier|
|rocksdb.max_total_wal_size|0(\[sum of all write_buffer_size * max_write_buffer_number\] * 4)|Sets the maximum total wal size in bytes. Once write-ahead logs exceed this size, we will start forcing the flush of column families whose memtables are backed by the oldest live WAL file (i.e. the ones that are causing all the space amplification)|
|rocksdb.memtable_huge_page_size|0|Sets the page size for huge page for arena used by the memtable|
|rocksdb.num_levels|7|Sets the number of levels for this database|
|rocksdb.use_direct_reads|false|Enable/Disable direct I/O mode (O_DIRECT) for reads|
|rocksdb.use_fsync|false|Enable/Disable fsync|
|rocksdb.write_buffer_size|64MB|Sets the amount of data to build up in memory (backed by an unsorted log on disk) before converting to a sorted on-disk file|
|rocksdb.max_write_buffer_number|2|Sets the maximum number of write buffers that are built up in memory|
|rocksdb.max_background_jobs|2|Sets maximum number of concurrent background jobs (compactions and flushes)|
|rocksdb.block_size|4KB|Sets the approximate size of user data packed per block. Note that the block size specified here corresponds opts uncompressed data. The actual size of the unit read from disk may be smaller if compression is enabled|
|rocksdb.block_size_deviation|10|Sets the block size deviation. This is used opts close a block before it reaches the configured 'block_size'. If the percentage of free space in the current block is less than this specified number and adding a new record opts the block will exceed the configured block size, then this block will be closed and the new record will be written opts the next block|
|rocksdb.cache_index_and_filter_blocks|false|Indicating if we'd put index/filter blocks to the block cache. If not specified, each "table reader" object will pre-load index/filter block during table initialization|
|rocksdb.no_block_cache|false|Specify whether block cache should be used or not|
|rocksdb.pin_l0_filter_and_index_blocks_in_cache|false|Sets cache_index_and_filter_blocks. If is true and the below is true (hash_index_allow_collision), then filter and index blocks are stored in the cache, but a reference is held in the "table reader" object so the blocks are pinned and only evicted from cache when the table reader is freed|
|rocksdb.whole_key_filtering|true|Specify if whole keys in the filter (not just prefixes) should be placed. This must generally be true for gets opts be efficient|
|rocksdb.block_restart_interval|16|Sets the number of keys between restart points for delta encoding of keys. This parameter can be changed dynamically|
|rocksdb.filter_policy|nil|Sets the filter policy opts reduce disk reads. Many applications will benefit from passing the result of NewBloomFilterPolicy() here|
|rocksdb.index_type|kBinarySearch|Sets the index type used for this table. __kBinarySearch__: A space efficient index block that is optimized for binary-search-based index. __kHashSearch__: The hash index, if enabled, will do the hash lookup when `Options.prefix_extractor` is provided. __kTwoLevelIndexSearch__: A two-level index implementation. Both levels are binary search indexes|
|rocksdb.block_align|false|Enable/Disable align data blocks on lesser of page size and block size|

### Spanner

|field|default value|description|
|-|-|-|
|spanner.db|""|Spanner Database|
|spanner.credentials|"~/.spanner/credentials.json"|Google application credentials for Spanner|

### Sqlite

|field|default value|description|
|-|-|-|
|sqlite.db|"/tmp/sqlite.db"|Database path|
|sqlite.mode|"rwc"|Open Mode: ro, rc, rwc, memory|
|sqlite.journalmode|"DELETE"|Journal mode: DELETE, TRUNCSTE, PERSIST, MEMORY, WAL, OFF|
|sqlite.cache|"Shared"|Cache: shared, private|

### Cassandra

|field|default value|description|
|-|-|-|
|cassandra.cluster|"127.0.0.1:9042"|Cassandra cluster|
|cassandra.keyspace|"test"|Keyspace|
|cassandra.connections|2|Number of connections per host|
|cassandra.username|cassandra|Username|
|cassandra.password|cassandra|Password|

### MongoDB

|field|default value|description|
|-|-|-|
|mongodb.url|"mongodb://127.0.0.1:27017"|MongoDB URI|
|mongodb.tls_skip_verify|false|Enable/disable server ca certificate verification|
|mongodb.tls_ca_file|""|Path to mongodb server ca certificate file|
|mongodb.namespace|"ycsb.ycsb"|Namespace to use|
|mongodb.authdb|"admin"|Authentication database|
|mongodb.username|N/A|Username for authentication|
|mongodb.password|N/A|Password for authentication|

### Redis
|field|default value|description|
|-|-|-|
|redis.datatype|hash|"hash", "string" or "json" ("json" requires [RedisJSON](https://redis.io/docs/stack/json/) available)|
|redis.mode|single|"single" or "cluster"|
|redis.network|tcp|"tcp" or "unix"|
|redis.addr||Redis server address(es) in "host:port" form, can be semi-colon `;` separated in cluster mode|
|redis.password||Redis server password|
|redis.db|0|Redis server target db|
|redis.max_redirects|8|The maximum number of retries before giving up (only for cluster mode)|
|redis.read_only|false|Enables read-only commands on slave nodes (only for cluster mode)|
|redis.route_by_latency|false|Allows routing read-only commands to the closest master or slave node (only for cluster mode)|
|redis.route_randomly|false|Allows routing read-only commands to the random master or slave node (only for cluster mode)|
|redis.max_retries||Max retries before giving up connection|
|redis.min_retry_backoff|8ms|Minimum backoff between each retry|
|redis.max_retry_backoff|512ms|Maximum backoff between each retry|
|redis.dial_timeout|5s|Dial timeout for establishing new connection|
|redis.read_timeout|3s|Timeout for socket reads|
|redis.write_timeout|3s|Timeout for socket writes|
|redis.pool_size|10|Maximum number of socket connections|
|redis.min_idle_conns|0|Minimum number of idle connections|
|redis.max_conn_age|0|Connection age at which client closes the connection|
|redis.pool_timeout|4s|Amount of time client waits for connections are busy before returning an error|
|redis.idle_timeout|5m|Amount of time after which client closes idle connections. Should be less than server timeout|
|redis.idle_check_frequency|1m|Frequency of idle checks made by idle connections reaper|
|redis.tls_ca||Path to CA file|
|redis.tls_cert||Path to cert file|
|redis.tls_key||Path to key file|
|redis.tls_insecure_skip_verify|false|Controls whether a client verifies the server's certificate chain and host name|

### BoltDB

|field|default value|description|
|-|-|-|
|bolt.path|"/tmp/boltdb"|The database file path. If the file does not exists then it will be created automatically|
|bolt.timeout|0|The amount of time to wait to obtain a file lock. When set to zero it will wait indefinitely. This option is only available on Darwin and Linux|
|bolt.no_grow_sync|false|Sets DB.NoGrowSync flag before memory mapping the file|
|bolt.read_only|false|Open the database in read-only mode|
|bolt.mmap_flags|0|Set the DB.MmapFlags flag before memory mapping the file|
|bolt.initial_mmap_size|0|The initial mmap size of the database in bytes. If <= 0, the initial map size is 0. If the size is smaller than the previous database, it takes no effect|

### etcd

|field|default value|description|
|-|-|-|
|etcd.endpoints|"localhost:2379"|The etcd endpoint(s), multiple endpoints can be passed separated by comma.|
|etcd.dial_timeout|"2s"|The dial timeout duration passed into the client config.|
|etcd.cert_file|""|When using secure etcd, this should point to the crt file.|
|etcd.key_file|""|When using secure etcd, this should point to the pem file.|
|etcd.cacert_file|""|When using secure etcd, this should point to the ca file.|

### DynamoDB

|field|default value|description|
|-|-|-|
|dynamodb.tablename|"ycsb"|The database tablename|
|dynamodb.primarykey|"_key"|The table primary key fieldname|
|dynamodb.rc.units|10|Read request units throughput|
|dynamodb.wc.units|10|Write request units throughput|
|dynamodb.ensure.clean.table|true|On load mode ensure that the table is clean at the begining. In case of true and if the table previously exists it will be deleted and recreated|
|dynamodb.endpoint|""|Used endpoint for connection. If empty will use the default loaded configs|
|dynamodb.region|""|Used region for connection ( should match endpoint ). If empty will use the default loaded configs|
|dynamodb.consistent.reads|false|Reads on DynamoDB provide an eventually consistent read by default. If your benchmark/use-case requires a strongly consistent read, set this option to true|
|dynamodb.delete.after.run.stage|false|Detele the database table after the run stage|



## TODO

- [ ] Support more measurement, like HdrHistogram
- [ ] Add tests for generators

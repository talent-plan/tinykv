## LAB1 StandaloneKV
In this lab, you will build a standalone key/value storage [gRPC](https://grpc.io/docs/guides/) service with the support of column family. Standalone means only a single node, not a distributed system. Column families like a logical namespace for the key/value pairs in the database, namely the values of the same key in different column families are not same.
The service supports four basic operations: Put/Delete/Get/Scan. It maintains a simple database of key/value pairs. Keys and values are strings. `Put` replaces the value for a particular key in the database, `Delete` deletes the key's value, `Get` fetches the current value for a key, and `Scan` fetches the current value for a series of keys. 
The lab can be broken down into 2 steps, including:
Implement a standalone storage engine.
Implement raw key/value service handlers.
#### The Code
The `gRPC` server is initialized in `kv/main.go` and it contains a `tinykv.Server` which provides a `gRPC` service named `TinyKv`. It was defined by [protocol-buffer](https://developers.google.com/protocol-buffers) in `proto/proto/tinykvpb.proto`, and the detail of rpc requests and responses are defined in `proto/proto/kvrpc.proto`.
Generally,  you don’t need to change the proto files because all necessary fields have been defined for you. But if you need to change some, modify the proto file and run `make proto` to update related generated go code in `proto/pkg/xxx.pb.go`. 
In addition, `Server` depends on a `Storage`, an interface you need to implement for our standalone storage engine located in `kv/storage/standalone_storage/standalone_storage.go`. Once the interface `Storage` is implemented in `StandaloneStorage`, you could implement the raw key/value service for our `Server` with it.
#### Implement standalone storage engine
The first mission is implementing a wrapper of badger key/value API. `Server`'s service is depending on an `Storage` which is defined in `kv/storage/storage.go`. In this context, the standalone storage engine is just a wrapper of badger key/value API which is provided by two methods:
``` go
type Storage interface {
    // Other stuffs
    Write(ctx *kvrpcpb.Context, batch []Modify) error
    Reader(ctx *kvrpcpb.Context) (StorageReader, error)
}
```
`Write` should provide a way that applies a series of modifications to the inner state which is, in this situation, a badger instance.
`Reader` should return a `StorageReader` that supports key/value's point get and scan operations on a snapshot.
Here are some hints for this step:
You can use (badger.Txn)[https://godoc.org/github.com/dgraph-io/badger#Txn] to implement the `Reader` function, because the transcation handler provided by badger could provide a consistent snapshot of the keys and values.
Badger doesn’t give support for column families. `engine_util`(kv/util/engine_util`) simulates column families by adding a prefix to keys.  For example, a key `key` that belongs to a specific column family `cf` is stored as `${cf}_${key}`.  So you should interact with badger through `engine_util` API.
There are two repositories with the name `badger`, just use github.com/Connor1996/badger, which is a fork of the original version. So that you can use functions in the  `engine_util` package. The `engine_util` package warps `badger` to provide operations with CFs, it also offers many useful helper functions.  You can read `util/engine_util/doc.go` to learn more.
#### Implement service handlers
The final step of this lab is to use the implemented storage engine to build raw key/value service handlers including RawGet/RawScan/RawPut/RawDelete. Once you have finished this implementation, remember to run `make lab1` to pass the test suite.


## Badger 

Useful badger stuct and method

### DB

1. `func Open(opt Options) (db *DB, err error)`
2. `func (db *DB) Close() error`
3. `func (db *DB) Backup(w io.Writer, since uint64) (uint64, error)`
4. `func (db *DB) Load(r io.Reader, maxPendingWrites int) error`
5. `func (db *DB) NewTransaction(update bool) *Txn`
6. `func (db *DB) NewWriteBatch() *WriteBatch`
7. `func (db *DB) Update(fn func(txn *Txn) error) error`
8. `func (db *DB) View(fn func(txn *Txn) error) error`

### Entry

This struct can be used by the user to set data

1. `func NewEntry(key, value []byte) *Entry`
2. `func (e *Entry) WithDiscard() *Entry`

### Item

Item is returned during iteration

1. `func (item *Item) ExpiresAt() uint64`
2. `func (item *Item) IsDeletedOrExpired() bool`
3. `func (item *Item) KeyCopy(dst []byte) []byte`
4. `func (item *Item) ValueCopy(dst []byte) ([]byte, error)`

### Iterator

Iterator helps iterating over the KV pairs in a lexicographically sorted order.

1. `func (it *Iterator) Item() *Item`
2. `func (it *Iterator) Close()`

### Txn

Txn represents a Badger transaction.

1. `func (txn *Txn) Commit() error`
2. `func (txn *Txn) Delete(key []byte) error`
3. `func (txn *Txn) Get(key []byte) (item *Item, rerr error)`
4. `func (txn *Txn) NewIterator(opt IteratorOptions) *Iterator`
5. `func (txn *Txn) Set(key, val []byte) error`

### WriteBatch

WriteBatch holds the necessary info to perform batched writes.

1. `func (wb *WriteBatch) Delete(k []byte) error`
2. `func (wb *WriteBatch) Flush() error`
3. `func (wb *WriteBatch) Set(k, v []byte) error`


## Column Families

Column Families provide a way to logically partition the database.
For example, when storing relationship tables by columns, different CFs are calibrated for different columns. Different CF strategies play different division effects.
Columns with the same CF may be stored in the same file.

## storage

### modify

```go
// ModifyType is the smallest unit of mutation of TinyKV's underlying storage (i.e., raw key/values on disk(s))
type Modify struct {
	Type ModifyType
	Data interface{}
}
```

### mem_storage

A reference for implementing storage interferce.

## engine_util

Encapsulated badger provides KV operations other than scan.

```go
// CreateDB creates a new Badger DB on disk at subPath.
func CreateDB(subPath string, conf *config.Config) *badger.DB 

// return the combination of given key and  cf
func KeyWithCF(cf string, key []byte) []byte 

//encapsulated Get
func GetCF(db *badger.DB, cf string, key []byte) (val []byte, err error)

//The actual GetCF internal implementation
func GetCFFromTxn(txn *badger.Txn, cf string, key []byte) (val []byte, err error)

//encapsulated Put
func PutCF(engine *badger.DB, cf string, key []byte, val []byte) error

//Get value for given key from DB and place it in msg
func GetMeta(engine *badger.DB, key []byte, msg proto.Message) error

//Get value for given key from msg and update it in DB
func PutMeta(engine *badger.DB, key []byte, msg proto.Message) error 

//Delete Key with 3 kinds CF between [startKey,endKey)
func DeleteRange(db *badger.DB, startKey, endKey []byte) error 

```


## Problem

1. A little more can be added to the CF-related function comments, as the use of CF is not very clear
2. There may not be much to suggest for the handling of request and response errors
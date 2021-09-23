# Project1 StandaloneKV

In this project, you will build a mini key-value storage service with the following characteristics.

#### Standalone Key-value storage

Standalone means only a single node, not a distributed system. This StandaloneKV in Project1 is just for storing key/value pairs locally and its underlying storage capability is built upon a pure go key-value database called badgerDB.

#### Support Column Family

[Column family]( <https://en.wikipedia.org/wiki/Standard_column_family> ) (abbreviate to CF below) denotes a logical layer between key-value entry and the database. You can consider CF as a key namespace, and multiple CFs are usually implemented as almost separate databases. Different values can be set for the same key in different CFs (CF store has some transactional property that is necessary for our transaction model in project 4).

#### Act as a gRPC service

The entire StandaloneKV should act as a [gRPC](https://grpc.io/docs/guides/) service. Don't stress too much if it sounds unfamiliar, the underlying structure has already been dealt with.

Generally, the service supports four basic operations: Put/Delete/Get/Scan. It maintains a simple database of key/value pairs. Keys and values are strings. 

- `Put` replaces the value for a particular key for the specified CF in the database.
- `Delete` deletes the key's value for the specified CF.
- `Get` fetches the current value for a key for the specified CF.
- `Scan` fetches the current value for a series of keys for the specified CF.

The project can be broken down into 2 steps, including:

1. Implement a standalone storage engine.
2. Implement raw key/value service handlers.

### The Code

`Server` depends on a `Storage`, an interface you need to implement for the standalone storage engine located in `kv/storage/standalone_storage/standalone_storage.go`. Once the interface `Storage` is implemented in `StandaloneStorage`, you could implement the raw key/value service for the `Server` with it.

For the storage engine, your code should be in **`kv/storage/standalone_storage.go`**. For service handlers, your code should be in **`kv/server/raw_api.go`**. Look for comments like `Your Code Here (1)`.


#### Implement standalone storage engine

The gRPC service depends on a `Storage` Interface defined at `kv/storage/storage.go`. Your job is to implement a wrapper for [badger](https://github.com/dgraph-io/badger)'s key/value API that satisfy this Storage Interface.

``` go
type Storage interface {
    // Other stuffs
    Write(ctx *kvrpcpb.Context, batch []Modify) error
    Reader(ctx *kvrpcpb.Context) (StorageReader, error)
}
```

`Write` should provide a way that applies a series of modifications to the inner state which is, in this situation, a badger instance.

In this function, you need handle a batch of `Modify`s, and write them into badgerDB together. Actually, `Modify` wraps the data required in one request, like the request type, CF, key, and value.  Moreover, `WriteBatch` (located in `kv/util/engine_util/write_batch.go`) would be a useful tool or reference for you to implement the `Write` process.

`Reader` should return a `StorageReader` that supports key/value's point get and scan operations on a snapshot. The actual implementation of the reader is up to you, but it must implement StorageReader's interface located in `kv/storage/standalone_storage`.

You should use [badger.Txn](https://godoc.org/github.com/dgraph-io/badger#Txn) to implement the `Reader` function because the transaction handler provided by badger could provide a consistent snapshot of the keys and values. You mainly need implement two functions:

- `GetCF` get value by CF and key from the request. Be careful if key was not found.
- `IterCF` create a new iterator with a CF. It is used for scanning a series of KV pairs in this CF.

And you don’t need to consider the `kvrpcpb.Context` now, it’s used in the following projects. 

> Hints:
>
> - Badger doesn't natively support Column Family store. So you should perform all read/ write operation through methods provided by engine_util package (`kv/util/engine_uti`). It simulates Column Family layer by adding prefix to keys (e.g., a `key` that belongs to a specific column family `cf` is stored as `${cf}_${key}`.
> - The engine_util package also provides many other useful helper functions, so make sure to take a look at them.
> - TinyKV uses a modified version of `badger`, so when needed, import from `github.com/Connor1996/badger` instead of `github.com/dgraph-io/badger`.
> - Don’t forget to call `Discard()` for badger.Txn and close all iterators before discarding.
> - If you are lost, take a look at `RaftStorage` and `RegionReader` under `/kv/storage/raft_storage` for reference.

#### Implement gRPC service handlers

The final step of this project is to use the implemented storage engine to build raw key/value service handlers including `RawGet`/ `RawScan`/ `RawPut`/ `RawDelete`. The handler is already defined for you, you only need to fill up the implementation in `kv/server/raw_api.go`.

Handlers get data from a Put/Delete/Get/Scan request, and return a response after processing. If an error occurred, it should be recorded in the response.

> Additional info about how the server is run:
>
> * The `gRPC` server is initialized in `kv/main.go` and it contains a `tinykv.Server` which provides a `gRPC` service named `TinyKv`. It was defined by [protocol-buffer](https://developers.google.com/protocol-buffers) in `proto/proto/tinykvpb.proto`, and the detail of rpc requests and responses are defined in `proto/proto/kvrpcpb.proto`.
> * We don't expect you to change any of the proto files. But if you still need to modify the proto file, make sure to run `make proto` to regenerate related `.go` file in `proto/pkg/xxx/xxx.pb.go`



Once you have finished both steps, you can run `make project1` from the project root to run all test suites. The relevant test cases are located in `kv/server/server_test.go`.

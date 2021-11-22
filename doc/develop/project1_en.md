# Project 1

## Target
1. Implement a standalone storage engine.
2. Implement raw key/value service handlers.

### standalone storage engine
The main task is to implement the `Storage` and `StorageReader` interface which defined in `kv/storage/storage.go`. The code to be implemented is placed in `kV/storage/standalone_storage/standalone_ storage.go` and `kV/storage/standalone_storage/reader.go`.

- `StandAloneStorage.NewStandAloneStorage`: return a `StandAloneStorage` structure, which is a wrapper of [badger](https://github.com/dgraph-io/badger)
- `StandAloneStorage.Stop`: close the database
- `StandAloneStorage.Write`: convert the `[]batch`  into a `WriteBatch` and then using the API in `engine_util.write_batch.go`, batch write to database
- `Reader.NewReader`: return a reader structure, which is a wrapper of [badger.Txn]( https://godoc.org/github.com/dgraph-io/badger#Txn ) 
- `Reader.GetCF:`using the `engine_util.GetCFFromTxn` API of `kv/util/engine_util`
- `Reader.IterCF:`using the `engine_util.NewCFIterator` API of `kv/util/engine_util`


### raw_api
The main task is to implement raw key/value service handlers. The code is placed in `kv/server/raw_api.go`.
- RawGet: Using `Reader.GetCF` to get value
- RawPut: First, use `Storage.Modify` to wrap `kvrpcpb.RawPutRequest` and then call `StandAloneStorage.Write` to write data to the database
- RawDelet: First, use `Storage.Modify` to wrap `kvrpcpb.RawPutRequest` and then call `StandAloneStorage.Write` to delete data from database
- RawScan: Use `Reader.IterCF` to get the iterator, then find the corresponding data and returng

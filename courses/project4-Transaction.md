# Project 4: Transactions

In the previous projects, you have built a key/value database which, by using Raft, is consistent across multiple nodes. To be truly scalable, the database must be able to handle multiple clients. With multiple clients, there is a problem: what happens if two clients try to write the same key at 'the same time'? If a client writes and then immediately reads that key, should they expect the read value to be the same as the written value? In project4, you will address such issues by building a transaction system into our database.

The transaction system will be a collaborative protocol between the client (TinySQL) and server (TinyKV). Both partners must be implemented correctly for the transactional properties to be ensured. We will have a complete API for transactional requests, independent of the raw request that you implemented in project1 (in fact, if a client uses both raw and transactional APIs, we cannot guarantee the transactional properties).

Transactions  promise [*snapshot isolation* (SI)](https://en.wikipedia.org/wiki/Snapshot_isolation). That means that within a transaction, a client will read from the database as if it were frozen in time at the start of the transaction (the transaction sees a *consistent* view of the database). Either all of a transaction is written to the database or none of it is (if it conflicts with another transaction).

To provide SI, you need to change the way that data is stored in the backing storage. Rather than storing a value for each key, you'll store a value for a key and a time (represented by a timestamp). This is called multi-version concurrency control (MVCC) because multiple different versions of a value are stored for every key.

You'll implement MVCC in part A. In parts B and C you’ll implement the transactional API.

## Transactions in TinyKV

TinyKV's transaction design follows [Percolator](https://storage.googleapis.com/pub-tools-public-publication-data/pdf/36726.pdf); it is a two phase commit protocol (2PC).

A transaction is a list of reads and writes. A transaction has a start timestamp and, when a transaction is committed, it has a commit timestamp (which must be greater than the starting timestamp). The whole transaction reads from the version of a key that is valid at the start timestamp. After committing, all writes appear to have been written at the commit timestamp. Any key to be written must not be written by any other transaction between the start and commit timestamps, otherwise the whole transaction is cancelled (this is called a write conflict).

The protocol starts with the client getting a start timestamp from TinyScheduler. It then builds the transaction locally, reading from the database (using a `KvGet` or `KvScan` request which includes the start timestamp, in contrast to  `RawGet` or `RawScan` requests), but only recording writes locally in memory. Once the transaction is built, the client will select one key as the *primary key* (note that this has nothing to do with an SQL primary key). The client sends `KvPrewrite` messages to TinyKV. A `KvPrewrite` message contains all the writes in the transaction. A TinyKV server will attempt to lock all keys required by the transaction. If locking any key fails, then TinyKV responds to the client that the transaction has failed. The client can retry the transaction later (i.e., with a different start timestamp). If all keys are locked, the prewrite succeeds. Each lock stores the primary key of the transaction and a time to live (TTL).

In fact, since the keys in a transaction may be in multiple regions and thus be stored in different Raft groups, the client will send multiple `KvPrewrite` requests, one to each region leader. Each prewrite contains only the modifications for that region. If all prewrites succeed, then the client will send a commit request for the region containing the primary key. The commit request will contain a commit timestamp (which the client also gets from TinyScheduler) which is the time at which the transaction's writes are committed and thus become visible to other transactions.

If any prewrite fails, then the transaction is rolled back by the client by sending a `KvBatchRollback` request to all regions (to unlock all keys in the transaction and remove any prewritten values).

In TinyKV, TTL checking is not performed spontaneously. To initiate a timeout check, the client sends the current time to TinyKV in a `KvCheckTxnStatus` request. The request identifies the transaction by its primary key and start timestamp. The lock may be missing or already be committed; if not, TinyKV compares the TTL on the lock with the timestamp in the `KvCheckTxnStatus` request. If the lock has timed out, then TinyKV rolls back the lock. In any case, TinyKV responds with the status of the lock so that the client can take action by sending a `KvResolveLock` request. The client typically checks transaction status when it fails to prewrite a transaction due to another transaction's lock.

If the primary key commit succeeds, then the client will commit all other keys in the other regions. These requests should always succeed because by responding positively to a prewrite request, the server is promising that if it gets a commit request for that transaction then it will succeed. Once the client has all its prewrite responses, the only way for the transaction to fail is if it times out, and in that case committing the primary key should fail. Once the primary key is committed, then the other keys can no longer time out.

If the primary commit fails, then the client will rollback the transaction by sending `KvBatchRollback` requests.

## Part A

The raw API you implemented in earlier projects maps user keys and values directly to keys and values stored in the underlying storage (Badger). Since Badger is not aware of the distributed transaction layer, you must handle transactions in TinyKV, and *encode* user keys and values into the underlying storage. This is implemented using multi-version concurrency control (MVCC). In this project, you will implement the MVCC layer in TinyKV.

Implementing MVCC means representing the transactional API using a simple key/value API. Rather than store one value per key, TinyKV stores every version of a value for each key. For example, if a key has value `10`, then gets set to `20`, TinyKV will store both values (`10` and `20`) along with timestamps for when they are valid.

TinyKV uses three column families (CFs): `default` to hold user values, `lock` to store locks, and `write` to record changes. The `lock` CF is accessed using the user key; it stores a serialized `Lock` data structure (defined in [lock.go](/kv/transaction/mvcc/lock.go)). The `default` CF is accessed using the user key and the *commit* timestamp of the transaction in which it was written; it stores the user value only. The `write` CF is accessed using the user key and the *start* timestamp of the transaction in which it was written; it stores a `Write` data structure (defined in [write.go](/kv/transaction/mvcc/write.go)).

A user key and timestamp are combined into an *encoded key*. Keys are encoded in such a way that an ascending order of encoded keys orders first by user key (ascending), then by timestamp (descending). This ensures that iterating over encoded keys will give the most recent version first. Helper functions for encoding and decoding keys are defined in [transaction.go](/kv/transaction/mvcc/transaction.go).

This exercise requires implementing a single struct called `MvccTxn`. In parts B and C, you'll use the `MvccTxn` API to implement the transactional API. `MvccTxn` provides read and write operations based on the user key and logical representations of locks, writes, and values. Modifications are collected in `MvccTxn` and once all modifications for a command are collected, they will be written all at once to the underlying database. This ensures that commands succeed or fail atomically. Note that an MVCC transaction is not the same as a TinySQL transaction. An MVCC transaction contains the modifications for a single command, not a sequence of commands.

`MvccTxn` is defined in [transaction.go](/kv/transaction/mvcc/transaction.go). There is a stub implementation, and some helper functions for encoding and decoding keys. Tests are in [transaction_test.go](/kv/transaction/mvcc/transaction_test.go). For this exercise, you should implement each of the `MvccTxn` methods so that all tests pass. Each method is documented with its intended behaviour.

> Hints:
>
> - An `MvccTxn` should know the start timestamp of the request it is representing.
> - The most challenging methods to implement are likely to be `GetValue` and the methods for retrieving writes. You will need to use `StorageReader` to iterate over a CF. Bear in mind the ordering of encoded keys, and remember that when deciding when a value is valid depends on the commit timestamp, not the start timestamp, of a transaction.

## Part B

In this part, you will use `MvccTxn` from part A to implement handling of `KvGet`, `KvPrewrite`, and `KvCommit` requests. As described above, `KvGet` reads a value from the database at a supplied timestamp. If the key to be read is locked by another transaction at the time of the `KvGet` request, then TinyKV should return an error. Otherwise, TinyKV must search the versions of the key to find the most recent, valid value.

`KvPrewrite` and `KvCommit` write values to the database in two stages. Both requests operate on multiple keys, but the implementation can handle each key independently.

`KvPrewrite` is where a value is actually written to the database. A key is locked and a value stored. We must check that another transaction has not locked or written to the same key.

`KvCommit` does not change the value in the database, but it does record that the value is committed. `KvCommit` will fail if the key is not locked or is locked by another transaction.

You'll need to implement the `KvGet`, `KvPrewrite`, and `KvCommit` methods defined in [server.go](/kv/server/server.go). Each method takes a request object and returns a response object, you can see the contents of these objects by looking at the protocol definitions in [kvrpcpb.proto](/proto/kvrpcpb.proto) (you shouldn't need to change the protocol definitions).

TinyKV can handle multiple requests concurrently, so there is the possibility of local race conditions. For example, TinyKV might receive two requests from different clients at the same time, one of which commits a key and the other rolls back the same key. To avoid race conditions, you can *latch* any key in the database. This latch works much like a per-key mutex. One latch covers all CFs. [latches.go](/kv/transaction/latches/latches.go) defines a `Latches` object which provides API for this.

> Hints:
>
> - All commands will be a part of a transaction. Transactions are identified by a start timestamp (aka start version).
> - Any request might cause a region error, these should be handled in the same way as for the raw requests. Most responses have a way to indicate non-fatal errors for situations like a key being locked. By reporting these to the client, it can retry a transaction after some time.

## Part C

In this part, you will implement `KvScan`, `KvCheckTxnStatus`, `KvBatchRollback`, and `KvResolveLock`. At a high-level, this is similar to part B - implement the gRPC request handlers in [server.go](/kv/server/server.go) using `MvccTxn`.

`KvScan` is the transactional equivalent of `RawScan`, it reads many values from the database. But like `KvGet`, it does so at a single point in time. Because of MVCC, `KvScan` is significantly more complex than `RawScan` - you can't rely on the underlying storage to iterate over values because of multiple versions and key encoding.

`KvCheckTxnStatus`, `KvBatchRollback`, and `KvResolveLock` are used by a client when it encounters some kind of conflict when trying to write a transaction. Each one involves changing the state of existing locks.

`KvCheckTxnStatus` checks for timeouts, removes expired locks, and returns the status of the lock.

`KvBatchRollback` checks that a key is locked by the current transaction, and if so removes the lock, deletes any value, and leaves a rollback indicator as a write.

`KvResolveLock` inspects a batch of locked keys and either rolls them all back or commits them all.

> Hints:
>
> - For scanning, you might find it helpful to implement your own scanner (iterator) abstraction which iterates over logical values, rather than the raw values in underlying storage. `kv/transaction/mvcc/scanner.go` is a framework for you.
> - When scanning, some errors can be recorded for an individual key and should not cause the whole scan to stop. For other commands, any single key causing an error should cause the whole operation to stop.
> - Since `KvResolveLock` either commits or rolls back its keys, you should be able to share code with the `KvBatchRollback` and `KvCommit` implementations.
> - A timestamp consists of a physical and a logical component. The physical part is roughly a monotonic version of wall-clock time. Usually we use the whole timestamp, for example when comparing timestamps for equality. However, when calculating timeouts, we must only use the physical part of the timestamp. To do this you may find the `PhysicalTime` function in [transaction.go](/kv/transaction/mvcc/transaction.go) useful.

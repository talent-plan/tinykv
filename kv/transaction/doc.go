package transaction

// The transaction package implements TinyKV's 'transaction' layer. This takes incoming requests from the tikv/server.go as
// input and turns them into reads and writes of the underlying key/value store (defined by Storage in tikv/server.go).
// The storage engine handles communicating with other nodes and writing data to disk. The transaction layer must
// translate high-level TinyKV commands into low-level raw key/value commands, schedule this processing to run efficiently,
// and ensure that processing of commands do not interfere with processing other commands.
//
// Note that there are two kinds of transactions in play: TinySQL transactions are collaborative between TinyKV and its
// client (e.g., TinySQL). They are implemented using multiple TinyKV commands and ensure that multiple SQL commands can
// be executed atomically. There are also mvcc transactions which are an implementation detail of this
// layer in TinyKV (represented by MvccTxn in tikv/transaction/mvcc/transaction.go). These ensure that a *single* TinySQL command
// is executed atomically.
//
// *Locks* are used to implement TinySQL transactions. Setting or checking a lock in a TinySQL transaction is lowered to
// writing or reading a key and value in the store.
//
// *Latches* are used to implement mvcc transactions and are not visible to the client. They are stored outside the
// underlying storage (or equivalently, you can think of every key having its own latch). See the latches package for details.
//
// Within this package, `commands` contains code to lower TinySQL requests to mvcc transactions. `mvcc` contains code for
// interacting with the underlying storage (Storage).
//
// Each transactional command is represented by a type which implements the `Command` interface and is defined in `commands`.
// See the `Command` docs for details on how a command is executed. The gRPC layer will handle each request on its own thread.
// We execute the command to completion on its own thread, relying on latches for thread safety. In TiKV there is a scheduler
// to optimise execution.
//
// Within the `mvcc` package, `Lock` and `Write` provide abstractions for lowering locks and writes into simple keys and values.
// `Scanner` is an abstraction for implementing the the transactional scan command - this is complicated because we must scan
// as if we were iterating over user key/values, rather than the encoding of these key/values which are stored in the DB.
//
// ## Encoding user key/values
//
// The mvcc strategy is essentially to store all data (committed and uncommitted) at every point in time. So for example, if we store
// a value for a key, then store another value (a logical overwrite) at a later time, both values are preserved in the underlying
// storage.
//
// This is implemented by encoding user keys with their timestamps (the starting timestamp of the transaction in which they are
// written) to make an encoded key (see codec.go). The `default` CF is a mapping from encoded keys to their values.
//
// Locking a key means writing into the `lock` CF. In this CF, we use the user key (i.e., not the encoded key so that a key is locked
// for all timestamps). The value in the `lock` CF consists of the 'primary key' for the transaction, the kind of lock (for 'put',
// 'delete', or 'rollback'), the start timestamp of the transaction, and the lock's ttl (time to live). See lock.go for the
// implementation.
//
// The status of values is stored in the `write` CF. Here we map keys encoded with their commit timestamps (i.e., the time at which a
// a transaction is committed) to a value containing the transaction's starting timestamp, and the kind of write ('put', 'delete', or
// 'rollback'). Note that for transactions which are rolled back, the start timestamp is used for the commit timestamp in the encoded
// key.

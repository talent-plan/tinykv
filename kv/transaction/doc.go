package transaction

// The transaction package implements TinyKV's 'transaction' layer. This takes incoming requests from the kv/server/server.go
// as input and turns them into reads and writes of the underlying key/value store (defined by Storage in kv/storage/storage.go).
// The storage engine handles communicating with other nodes and writing data to disk. The transaction layer must
// translate high-level TinyKV commands into low-level raw key/value commands and ensure that processing of commands do
// not interfere with processing other commands.
//
// Note that there are two kinds of transactions in play: TinySQL transactions are collaborative between TinyKV and its
// client (e.g., TinySQL). They are implemented using multiple TinyKV requests and ensure that multiple SQL commands can
// be executed atomically. There are also mvcc transactions which are an implementation detail of this
// layer in TinyKV (represented by MvccTxn in kv/transaction/mvcc/transaction.go). These ensure that a *single* request
// is executed atomically.
//
// *Locks* are used to implement TinySQL transactions. Setting or checking a lock in a TinySQL transaction is lowered to
// writing to the underlying store.
//
// *Latches* are used to implement mvcc transactions and are not visible to the client. They are stored outside the
// underlying storage (or equivalently, you can think of every key having its own latch). See the latches package for details.
//
// Within the `mvcc` package, `Lock` and `Write` provide abstractions for lowering locks and writes into simple keys and values.
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

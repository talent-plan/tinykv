package transaction

// The transaction package implements TinyKV's 'transaction' layer. This takes incoming requests from the tikv/server.go as
// input and turns them into reads and writes of the underlying key/value store (defined by InnerServer in tikv/server.go).
// The InnerServer handles communicating with other nodes and writing data to disk. The transaction layer must
// translate high-level TinyKV commands into low-level raw key/value commands, schedule this processing to run efficiently,
// and ensure that processing of commands do not interfere with processing other commands.
//
// Note that there are two kinds of transactions in play: TinySQL transactions are collaborative between TinyKV and its
// client (e.g., TinySQL). They are implemented using multiple TinyKV commands and ensure that multiple SQL commands can
// be executed atomically. There are also mvcc transactions which are an implementation detail of this
// layer in TinyKV (represented by Txn in tikv/storage/exec/transaction.go). These ensure that a *single* TinyKV command
// is executed atomically.
//
// *Locks* are used to implement TinySQL transactions. Setting or checking a lock in a TinySQL transaction is lowered to
// writing or reading a key and value in the InnerServer store. TODO explain this encoding in detail.
//
// *Latches* are used to implement mvcc transactions and are not visible to the client. They are stored outside the
// underlying storage (or equivalently, you can think of every key having its own latch). TODO explain latching in more detail.
//
// Within this package, `commands` contains code to lower TinySQL requests to mvcc transactions. `mvcc` contains code for interacting with the underlying storage
// (InnerServer).

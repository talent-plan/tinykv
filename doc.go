package tinykv

/*
TinyKV is a distributed key/value store intended for teaching and experimentation. It is not suitable for production
use. It is based on the [TiKV](https://github.com/tikv/tikv) project (and we suggest you use that if you need a key/value
store for use in production).

Compared to TiKV, TinyKV is smaller, simpler, less robust, less performant, and less feature-rich. It is written
entirely in Go.

Building TinyKV produces two executables: unistore-server and pd-server. The first is one node of the key/value server,
the second is a 'placement driver', based on the [PD](https://github.com/pingcap/pd) project. PD is responsible for
managing TinyKV nodes (e.g., splitting and merging shards) and for generating timestamps.

The `tinykv` module is organized into the following packages:

* `kv`: implementation of the TinyKV key/value store.
* `proto`: all communication between nodes and process uses Protocol Buffers over gRPC. This package contains the protocol
  definitions used by TinyKV, and Go code for using them.
* `raft`: an implementation of the Raft distributed consensus algorithm, used in TinyKV.
* `scheduler`: implementation of the PD server.
 */

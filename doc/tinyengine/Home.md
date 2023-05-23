# Welcome to the TinyEngine!
TinyEngine is a course designed to help you quickly familiarize yourself with the underlying storage engine of [TiKV Project](https://github.com/tikv/tikv). 

After completing this course, you will have a better understanding of LSM-tree-based KV stores with high throughput, high scalability and high space utilization.

## Course Introduction
TinyEngine is forked from open source [TinyKV](https://github.com/talent-plan/tinykv), a key-value storage system with the Raft consensus algorithm. TinyKV focuses on the storage layer of a distributed database system, which uses [badger](https://github.com/dgraph-io/badger), a Go library to store keys and values, as its storage engine. In order to get closer to the actual implementation of TiKV, TinyEngine plans to replace the original storage engine badger with [LevelDB](https://github.com/google/leveldb)/[RocksDB](https://github.com/facebook/rocksdb) wrapped by Golang. Therefore, please modify your implementation of project1 to use the interface of levigo(a wrapper of LevelDB) or gorocksdb(a wrapper of RocksDB) rather than badger.

In this course, you need to finish the implementation of LevelDB and then implement an existing optimization method on LevelDB/RocksDB. We provide a KV
Separation project [WiscKey](https://dl.acm.org/doi/abs/10.1145/3033273) in this folder, which introduce a classic and generally accepted optimization idea presented in recent famous paper. Please implement it on LevelDB. 

After completing the implementation, you need to test and evaluate your optimization. We provide go-ycsb, which can be used to evaluate database performance. If you successfully implement a project, you will get better performance in reading or writing or some other dimension. Finally, you need to chart your evaluation results and submit a report and source code. The experts will give you an appropriate score depending on your optimization results and report. 

After finish this course, you can try to implement other optimizations like [DiffKV](chrome-extension://efaidnbmnnnibpcajpcglclefindmkaj/https://www.usenix.org/system/files/atc21-li-yongkun.pdf).

### LevelDB/RocksDB
[LevelDB](https://github.com/google/leveldb)/[RocksDB](https://github.com/facebook/rocksdb) is a storage engine for server workloads on various storage media, with the initial focus on fast storage (especially Flash storage). It is a C++ library to store key-value pairs. It supports both point lookups and range scans, and provides different types of ACID guarantees.

RocksDB borrows significant code from the open source [Leveldb](https://code.google.com/google/leveldb/) project and does a lot of performance optimization. It performs better than LevelDB under many real workloads and TiKV uses it as storage engine. However, RocksDB has a higher amount of code and is more difficult to learn. As a beginner's course for KV storage, TinyEngine requires you to complete the course with LevelDB. Besides, if you already have the knowledge of LSM-tree or enough coding ability, TinyEngine also provide code based on the RocksDB.

## Evaluation & Report
After finishing the project, you need to present evaluation results that demonstrate the benefits of your optimization and write a report. After submitting, you will receive a score based on the optimization results and your report.

### Go-ycsb 
Go-ycsb is a Go port of YCSB. It fully supports all YCSB generators and the Core workload so we can do the basic CRUD benchmarks with Go. Please follow [go-ycsb](https://github.com/pingcap/go-ycsb/blob/master/README.md) to understand it. There are different workloads can be used in the go-ycsb/workloads folder. The specific parameters can be changed if needed.

## Build TinyEngine from Source

### Prerequisites

* `git`: The source code of TinyEngine is hosted on GitHub as a git repository. To work with git repository, please [install `git`](https://git-scm.com/downloads).
* `go`: TinyEngine is a Go project. To build TinyEngine from source, please [install `go`](https://golang.org/doc/install) with version greater or equal to 1.13.
* `leveldb`: LevelDB is a storage engine of TinyEngine, please [install `leveldb`](https://github.com/google/leveldb) with version greater or equal to 1.7.
* `rocksdb`: RocksDB is also a storage engine of TinyEngine, please [install `rocksdb`](https://github.com/facebook/rocksdb) with version greater or equal to 5.16.

### Clone

Clone the source code to your development machine.

```bash
git clone https://github.com/QingyangZ/tinydb
```

### Build

Build TinyEngine from the source code.

```bash
cd tinydb
make
```

### Go-ycsb Test

Build go-ycsb and test TinyEngine

```bash
cd go-ycsb
make
```

#### Load

```bash
./bin/go-ycsb load tinydb -P workloads/workloada
```

#### Run
```bash
./bin/go-ycsb run tinydb -P workloads/workloada
```
 
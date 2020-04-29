# The TinyKV Course

This is a series of projects on a key-value storage system built with the Raft consensus algorithm. These projects are inspired by the famous [MIT 6.824](http://nil.csail.mit.edu/6.824/2018/index.html) course, but aim to be closer to industry implementations. The whole course is pruned from [TiKV](https://github.com/tikv/tikv) and re-written in Go. After completing this course, you will have the knowledge to implement a basic key-value storage fault-tolerance service with distributed transaction support and a better understanding of TiKV implementation.

The whole project is a skeleton code for a kv server and a scheduler server at initial, and you need to finish the core logic step by step:

- Project1: build a standalone key-value server
- Project2: build a fault tolerant key-value server with Raft
- Project3: support multi Raft group and balance scheduling on top of Project2
- Project4: support distributed transaction on top of Project3

**Important note: This course is still in developing, and the document is incomplete.** Any feedback and contribution is greatly appreciated. Please see help wanted issues if you want to join in the development.

## Course

Before starting, you’d better read the overview design of TiKV and PD to get a general impression on what you will build:

- TiKV
  - <https://pingcap.com/blog-cn/tidb-internal-1/> (Chinese Version)
  - <https://pingcap.com/blog/2017-07-11-tidbinternal1/> (English Version)
- PD
  - <https://pingcap.com/blog-cn/tidb-internal-3/> (Chinese Version)
  - <https://pingcap.com/blog/2017-07-20-tidbinternal3/> (English Version)

### Getting started

First, please clone the repository with git to get the source code of the project.

``` bash
git clone https://github.com/pingcap-incubator/tinykv.git
```

Then make sure you have installed [go](https://golang.org/doc/install) >= 1.13 toolchains. You should also have installed `make`.
Now you can run `make` to check that everything is working as expected. You should see it runs successfully.

### Overview of the code

![overview](courses/imgs/overview.png)

Same as the architect TiDB + TiKV + PD that separates the storage and computation, TinyKV only focuses on the storage layer of a distributed database system. If you are also interested in SQL layer, see [TinySQL](https://github.com/pingcap-incubator/tinysql). Besides that, there is a component called TinyScheduler as a center control of the whole TinyKV cluster, which collects information from the heartbeats of TinyKV. After that, the TinyScheduler can generate some scheduling tasks and distribute them to the TinyKV instances. All of them are communicated by RPC.

The whole project is organized into the following directories:

- `kv`: implementation of the TinyKV key/value store.
- `proto`: all communication between nodes and processes uses Protocol Buffers over gRPC. This package contains the protocol definitions used by TinyKV, and generated Go code for using them.
- `raft`: implementation of the Raft distributed consensus algorithm, used in TinyKV.
- `scheduler`: implementation of the TinyScheduler which is responsible for managing TinyKV nodes and for generating timestamps.

### Course material

- [Project1 - StandaloneKV](courses/project1-StandaloneKV.md)
- [Project2 - RaftKV](courses/project2-RaftKV.md)
- [Project3 - MultiRaftKV](courses/project3-MultiRaftKV.md)
- [Project4 - Transaction](courses/project4-Transaction.md)

## Deploy a cluster

Rather than a course, you can try TinyKV by deploying a real cluster, and interact with it through TinySQL.

### Build

```
make
```

It builds the binary of `tinykv-server` and `tinyscheduler-server` to `bin` dir.

### Run

Put the binary of `tinyscheduler-server`, `tinykv-server` and `tinysql-server` into a single dir.

Under the binary dir, run the following commands:

```
mkdir -p data
```

```
./tinyscheduler-server
```

```
./tinykv-server -path=data
```

```
./tinysql-server --store=tikv --path="127.0.0.1:2379"
```

### Play

```
mysql -u root -h 127.0.0.1 -P 4000
```

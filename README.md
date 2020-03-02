# The TinyKV LAB
This is a series of labs on a key-value storage system built with the Raft consensus algorithm. These labs are inspired by the famous [MIT  6.824](http://nil.csail.mit.edu/6.824/2018/index.html) course, but aim to be closer to industry implementations. The whole lab is pruned from [TiKV](github.com/tikv/tikv) and re-written in Go. After completing this course, you will have the knowledge to implement a basic key-value storage service with distributed transactions and fault-tolerance and better understanding of TiKV implementation.

The whole project is a skeleton code for a kv server and a scheduler server at initial, and you need to finish the core logic step by step:
- LAB1: build a standalone key-value server
- LAB2: build a fault tolerant key-value server with Raft
- LAB3: support multi Raft group and balance scheduling on top of LAB2
- LAB4: support distributed transaction on top of LAB3

**Important note: This course is still in developing, and the document is incomplete.** Any feedback and contribution is greatly appreciated. Please see help wanted issues if you want to join in the development.

## Build
```
make
```

## Test
```
make test
```

## Run(Not runnable now)

Put the binary of `tinyscheduler-server`, `tinykv-server` and `tidb-server` into a single dir.

Under the binary dir, run the following commands:

```
mkdir -p data
```

```
./tinyscheduler-server
```

```
./tinykv-server --db-path=data
```

```
./tinysql-server --store=tikv --path="127.0.0.1:2379"
```

## Documentation(Incomplete)

This repo contains a single module: tinykv. Each package is documented either in a doc.go file or, if it is a single
file package, in the single file.

See [doc.go](doc.go) for top-level documentation.

# unistore

A fun project for evaluating some new optimizations quickly, do not use it in production.

## Build
```
make
```

## Deploy

Put the binary of `pd-server`, `node` and `tidb-server` into a single dir.

## Run

Under the binary dir, run the following commands:

```
mkdir -p data
```

```
./pd-server
```

```
./node --db-path=data
```

```
./tidb-server --store=tikv --path="127.0.0.1:2379"
```

## Documentation

This repo contains a single module: tinykv. Each package is documented either in a doc.go file or, if it is a single
file package, in the single file.

See [doc.go](doc.go) for top-level documentation.

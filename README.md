# unistore
A fun project for evaluating some new optimizations quickly, do not use it in production.


## Build
```
make
```

## Deploy

Put the binary of `pd-serever`, `node` and `tidb-server` into a single dir.

## Run

Under the binary dir, run this script.

```
mkdir -p data
nohup ./pd-server --name="pd" --data-dir="pd" --log-file=pd.log
sleep 2
nohup ./node --db-path=data --vlog-path=data &
sleep 2
nohup ./tidb-server -P 4000 --store=tikv --path="127.0.0.1:2379" --log-file=tidb.log  &
```

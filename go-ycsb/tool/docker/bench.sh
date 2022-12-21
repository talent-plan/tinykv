#!/bin/bash

TYPE=$1
DB=$2

# Direcotry to save logs
LOG=./logs

RECORDCOUNT=100000
OPERATIONCOUNT=100000
THREADCOUNT=20
FIELDCOUNT=5
FIELDLENGTH=16
MAXSCANLENGTH=10

PROPS="-p recordcount=${RECORDCOUNT} \
    -p operationcount=${OPERATIONCOUNT} \
    -p threadcount=${THREADCOUNT} \
    -p fieldcount=${FIELDCOUNT} \
    -p fieldlength=${FIELDLENGTH} \
    -p maxscanlength=${MAXSCANLENGTH}"
PROPS+=" ${@:3}"
WORKLOADS=
SLEEPTIME=10

mkdir -p ${LOG} 

BENCH_DB=${DB}

case ${DB} in
    mysql)
        PROPS+=" -p mysql.host=mysql"
        SLEEPTIME=30
        ;;
    mysql8)
        PROPS+=" -p mysql.host=mysql"
        SLEEPTIME=30
        DB="mysql"
        ;;
    mariadb)
        PROPS+=" -p mysql.host=mariadb"
        SLEEPTIME=60
        DB="mysql"
        ;;
    pg)
        PROPS+=" -p pg.host=pg"
        SLEEPTIME=30
        ;;
    tikv)
        PROPS+=" -p tikv.pd=pd:2379 -p tikv.type=txn"
        ;;
    raw)
        PROPS+=" -p tikv.pd=pd:2379 -p tikv.type=raw"
        DB="tikv"
        ;;
    tidb)
        PROPS+=" -p mysql.host=tidb -p mysql.port=4000"
        ;;
    cockroach)
        PROPS+=" -p pg.host=cockroach -p pg.port=26257"
        ;;
    sqlite)
        PROPS+=" -p sqlite.db=/data/sqlite.db"
        ;;
    cassandra)
        PROPS+=" -p cassandra.cluster=cassandra"
        SLEEPTIME=30
        ;;
    scylla)
        PROPS+=" -p cassandra.cluster=scylla"
        SLEEPTIME=30
        ;;
    *)
    ;;
esac

echo ${TYPE} ${DB} ${WORKLOADS} ${PROPS}

CMD="docker-compose -f ${BENCH_DB}.yml" 

if [ ${TYPE} == 'load' ]; then 
    $CMD down --remove-orphans
    rm -rf ./data/${BENCH_DB}
    $CMD up -d
    sleep ${SLEEPTIME}

    $CMD run ycsb load ${DB} ${WORKLOADS} -p workload=core ${PROPS} | tee ${LOG}/${BENCH_DB}_load.log

    $CMD down
elif [ ${TYPE} == 'run' ]; then
    $CMD up -d
    sleep ${SLEEPTIME}

    for workload in a b c d e f 
    do 
        $CMD run --rm ycsb run ${DB} -P ../../workloads/workload${workload} ${WORKLOADS} ${PROPS} | tee ${LOG}/${BENCH_DB}_workload${workload}.log
    done

    $CMD down
else
    echo "invalid type ${TYPE}"
    exit 1
fi 

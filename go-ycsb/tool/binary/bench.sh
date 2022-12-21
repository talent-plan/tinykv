#!/bin/bash

TYPE=$1
DB=$2

# Directory to save data
DATA=./data
CMD=../../bin/go-ycsb
# Direcotry to save logs
LOG=./logs

RECORDCOUNT=100000000
OPERATIONCOUNT=100000000
THREADCOUNT=16
FIELDCOUNT=10
FIELDLENGTH=100
MAXSCANLENGTH=10

PROPS="-p recordcount=${RECORDCOUNT} \
    -p operationcount=${OPERATIONCOUNT} \
    -p threadcount=${THREADCOUNT} \
    -p fieldcount=${FIELDCOUNT} \
    -p fieldlength=${FIELDLENGTH} \
    -p maxscanlength=${MAXSCANLENGTH}"
PROPS+=" ${@:3}"
WORKLOADS=

mkdir -p ${LOG} 

DBDATA=${DATA}/${DB}

if [ ${DB} == 'rocksdb' ]; then 
    PROPS+=" -p rocksdb.dir=${DBDATA}"
    WORKLOADS="-P property/rocksdb"
elif [ ${DB} == 'badger' ]; then 
    PROPS+=" -p badger.dir=${DBDATA}"
    WORKLOADS="-P property/badger"
fi


if [ ${TYPE} == 'load' ]; then 
    echo "clear data before load"
    PROPS+=" -p dropdata=true"
fi 

echo ${TYPE} ${DB} ${WORKLOADS} ${PROPS}

if [ ${TYPE} == 'load' ]; then 
    $CMD load ${DB} ${WORKLOADS} -p=workload=core ${PROPS} | tee ${LOG}/${DB}_load.log
elif [ ${TYPE} == 'run' ]; then
    for workload in a b c d e f 
    do 
        $CMD run ${DB} -P ../../workloads/workload${workload} ${WORKLOADS} ${PROPS} | tee ${LOG}/${DB}_workload${workload}.log
    done
else
    echo "invalid type ${TYPE}"
    exit 1
fi 


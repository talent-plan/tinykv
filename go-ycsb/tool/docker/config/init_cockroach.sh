#!/bin/bash
echo "Wait for servers to be up"
sleep 5

HOSTPARAMS="--host cockroach --insecure"
SQL="/cockroach/cockroach.sh sql $HOSTPARAMS"

$SQL -e "CREATE DATABASE test;"
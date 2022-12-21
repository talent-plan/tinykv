#!/bin/bash
echo "Wait for servers to be up"

for _ in {1..10}
do
    sleep 5
    cqlsh $1 -e "CREATE KEYSPACE test WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor' : 1};"
    if [ $? -eq 0 ] 
    then
        echo "create keyspace ok"
        break
    fi
    echo "create keyspace failed, wait 5s"
done

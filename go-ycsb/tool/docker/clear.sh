
for db in pg cockroach mysql mysql8 tidb tikv
do
    docker-compose -f ${db}.yml down --remove-orphans
done

rm -rf ./data
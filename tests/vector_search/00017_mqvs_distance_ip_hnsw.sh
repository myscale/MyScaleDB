#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

clickhouse-client -q "DROP TABLE IF EXISTS test_vector;"
clickhouse-client -q "CREATE TABLE test_vector(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 4) engine MergeTree primary key id;"
clickhouse-client -q "ALTER TABLE test_vector ADD VECTOR INDEX v1 vector TYPE HNSWFLAT('metric_type=IP');"
clickhouse-client -q "INSERT INTO test_vector select number, [number / pow(number, 2), number / pow(number, 2), number / pow(number, 2), sqrt(1 - 3 * pow(number / pow(number, 2), 2))] from numbers(4000) where number > 1;"

status="NotBuilt"
time=0
while [[ $status != "Built" && $time != 10 ]]
do
        status=`clickhouse-client -q "select status from system.vector_indices where table = 'test_vector' and name = 'v1';"`
        sleep 1
        ((++time))
done
if [ $time -eq 10 ]; then
        echo "fail to build index"
fi

clickhouse-client -q "SELECT id, vector, distance('topK=10')(vector, [0.5, 0.5, 0.5, 0.5]) FROM test_vector;"
clickhouse-client -q "DROP TABLE IF EXISTS test_vector;"
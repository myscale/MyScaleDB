#!/usr/bin/env bash

clickhouse-client -q "DROP TABLE IF EXISTS test_vector"
clickhouse-client -q "CREATE TABLE test_vector(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) engine MergeTree primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=0, vector_index_parameter_check=0;"
clickhouse-client -q "INSERT INTO test_vector SELECT number, [number, number, number] FROM numbers(10000);"
clickhouse-client -q "ALTER TABLE test_vector ADD VECTOR INDEX v1 vector TYPE IVFSQ;"
status="NotBuilt"
time=0
while [[ $status != "Built" && $time != 5 ]]
do
        status=`clickhouse-client -q "select status from system.vector_indices where table = 'test_vector' and name = 'v1';"`
        sleep 1
        ((++time))
done
if [ $time -eq 5 ]; then
        echo "fail to build index"
fi

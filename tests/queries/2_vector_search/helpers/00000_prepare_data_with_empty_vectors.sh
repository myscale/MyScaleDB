#!/usr/bin/env bash

# no enforce_fixed_vector_length_constraint
clickhouse-client -q "DROP TABLE IF EXISTS test_vector"
clickhouse-client -q "CREATE TABLE test_vector(id Float32, vector Array(Float32)) engine MergeTree primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=0, enforce_fixed_vector_length_constraint=0, vector_index_parameter_check=0;"
clickhouse-client -q "INSERT INTO test_vector SELECT number, [number, number, number] FROM numbers(10);"
clickhouse-client -q "INSERT INTO test_vector SELECT number + 10, [] FROM numbers(20);"
clickhouse-client -q "INSERT INTO test_vector SELECT number + 30, [number + 30, number + 30, number + 30] FROM numbers(400);"
clickhouse-client -q "OPTIMIZE TABLE test_vector;"
clickhouse-client -q "ALTER TABLE test_vector ADD CONSTRAINT vector_len CHECK length(vector) = 3;"
clickhouse-client -q "ALTER TABLE test_vector ADD VECTOR INDEX v1 vector TYPE IVFFLAT('ncentroids = 10');"
status="NotBuilt"
time=0
while [[ $status != "Built" && $time != 5 ]]; do
        status=$(clickhouse-client -q "select status from system.vector_indices where table = 'test_vector' and name = 'v1';")
        sleep 1
        ((++time))
done
if [ $time -eq 5 ]; then
        echo "fail to build index"
fi

# default enforce_fixed_vector_length_constraint
# empty array
clickhouse-client -q "DROP TABLE IF EXISTS test_fail_vector"
clickhouse-client -q "CREATE TABLE test_fail_vector(id Float32, vector Array(Float32)) engine MergeTree primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=0, vector_index_parameter_check=0;"
clickhouse-client -q "INSERT INTO test_fail_vector SELECT number, [number, number, number] FROM numbers(10);"
clickhouse-client -q "INSERT INTO test_fail_vector SELECT number + 10, [] FROM numbers(20);"
clickhouse-client -q "INSERT INTO test_fail_vector SELECT number + 30, [number + 30, number + 30, number + 30] FROM numbers(400);"
clickhouse-client -q "OPTIMIZE TABLE test_fail_vector;"
clickhouse-client -q "ALTER TABLE test_fail_vector ADD CONSTRAINT vector_len CHECK length(vector) = 3;"
clickhouse-client -q "ALTER TABLE test_fail_vector ADD VECTOR INDEX v1_fail vector TYPE IVFFLAT('ncentroids = 10');"

# special case
clickhouse-client -q "DROP TABLE IF EXISTS test_fail_vector_2"
clickhouse-client -q "CREATE TABLE test_fail_vector_2(id Float32, vector Array(Float32)) engine MergeTree primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=0, vector_index_parameter_check=0;"
clickhouse-client -q "INSERT INTO test_fail_vector_2 SELECT number, [number, number, number] FROM numbers(10);"
clickhouse-client -q "INSERT INTO test_fail_vector_2 SELECT number + 10, [number + 10, number + 10] FROM numbers(210);"
clickhouse-client -q "INSERT INTO test_fail_vector_2 SELECT number + 220, [number + 220, number + 220, number + 220, number + 220] FROM numbers(210);"
clickhouse-client -q "OPTIMIZE TABLE test_fail_vector_2;"
clickhouse-client -q "ALTER TABLE test_fail_vector_2 ADD CONSTRAINT vector_len CHECK length(vector) = 3;"
clickhouse-client -q "ALTER TABLE test_fail_vector_2 ADD VECTOR INDEX v1_fail_2 vector TYPE IVFFLAT('ncentroids = 10');"

#!/usr/bin/env bash

clickhouse-client -q "DROP TABLE IF EXISTS test_vector"
clickhouse-client -q "CREATE TABLE test_vector(id Float32, vector Array(Float32)) engine MergeTree primary key id SETTINGS index_granularity=128, min_rows_to_build_vector_index=0, vector_index_parameter_check=0;"
clickhouse-client -q "INSERT INTO test_vector SELECT number, [number, number, number] FROM numbers(10);"
clickhouse-client -q "INSERT INTO test_vector SELECT number + 10, [] FROM numbers(20);"
clickhouse-client -q "INSERT INTO test_vector SELECT number + 30, [number + 30, number + 30, number + 30] FROM numbers(10000);"
clickhouse-client -q "ALTER TABLE test_vector ADD CONSTRAINT vector_len CHECK length(vector) = 3;"

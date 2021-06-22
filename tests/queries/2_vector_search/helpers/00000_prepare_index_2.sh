#!/usr/bin/env bash

clickhouse-client -q "DROP TABLE IF EXISTS test_vector"
clickhouse-client -q "CREATE TABLE test_vector(id Float32, vector FixedArray(Float32, 3)) engine MergeTree primary key id SETTINGS index_granularity=128;"
clickhouse-client -q "INSERT INTO test_vector SELECT number, [number, number, number] FROM numbers(10);"
clickhouse-client -q "INSERT INTO test_vector SELECT number + 10, [] FROM numbers(20);"
clickhouse-client -q "INSERT INTO test_vector SELECT number + 30, [number + 30, number + 30, number + 30] FROM numbers(10000);"

#!/usr/bin/env bash
# Tags: no-parallel

clickhouse-client -q "DROP TABLE IF EXISTS test_ivfflat"
clickhouse-client -q "CREATE TABLE test_ivfflat(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) engine MergeTree primary key id SETTINGS index_granularity=1024;"
clickhouse-client -q "ALTER TABLE test_ivfflat ADD VECTOR INDEX v1 vector TYPE IVFFLAT('ncentroids = 1');"
clickhouse-client -q "SELECT table,name,type,expr,status from system.vector_indices where table = 'test_ivfflat'"
clickhouse-client -q "DROP TABLE test_ivfflat"

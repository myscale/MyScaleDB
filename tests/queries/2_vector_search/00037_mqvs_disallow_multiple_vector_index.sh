#!/usr/bin/env bash
# Tags: no-parallel

# test single vector column table
clickhouse-client -q "DROP TABLE IF EXISTS test_single_vector;"
clickhouse-client -q "SELECT 'test single vector column table';"
clickhouse-client -q "CREATE TABLE test_single_vector(id UInt32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3)engine = MergeTree PRIMARY KEY id;"

clickhouse-client -q "SELECT 'add a vector index';"
clickhouse-client -q "ALTER TABLE test_single_vector ADD VECTOR INDEX v1 vector TYPE HNSWFLAT;" 2>&1 | grep -q "DB::Exception" && echo 'FAIL' || echo 'OK' || :
sleep 1
clickhouse-client -q "SELECT 'add other vector index';"
clickhouse-client -q "ALTER TABLE test_single_vector ADD VECTOR INDEX v2 vector TYPE MSTG;" 2>&1 | grep -q "DB::Exception: Cannot add vector index v2: this column already has a vector index definition." && echo 'OK' || echo 'FAIL' || :
sleep 1
clickhouse-client -q "SELECT 'create a vector index with if not exists';"
clickhouse-client -q "CREATE VECTOR INDEX IF NOT EXISTS v3 ON test_single_vector vector TYPE FLAT;" 2>&1 | grep -q "DB::Exception" && echo 'FAIL' || echo 'OK' || :
sleep 1
clickhouse-client -q "SELECT 'create a vector index';"
clickhouse-client -q "CREATE VECTOR INDEX v4 ON test_single_vector vector TYPE FLAT;" 2>&1 | grep -q "DB::Exception: Cannot add vector index v4: this column already has a vector index definition." && echo 'OK' || echo 'FAIL' || :
sleep 1
clickhouse-client -q "SELECT table, name, type FROM system.vector_indices WHERE table='test_single_vector';"
clickhouse-client -q "DROP TABLE IF EXISTS test_single_vector;"

# test multiple vector columns table
clickhouse-client -q "DROP TABLE IF EXISTS test_multiple_vector;"
clickhouse-client -q "SELECT 'test multiple vector columns table';"
clickhouse-client -q "CREATE TABLE test_multiple_vector(id UInt32, vector1 Array(Float32), vector2 Array(Float32), CONSTRAINT vector1_len CHECK length(vector1) = 3, CONSTRAINT vector2_len CHECK length(vector2) = 3)engine = MergeTree PRIMARY KEY id;"

clickhouse-client -q "SELECT 'add a vector index on vector1 column';"
clickhouse-client -q "ALTER TABLE test_multiple_vector ADD VECTOR INDEX v1 vector1 TYPE HNSWFLAT;" 2>&1 | grep -q "DB::Exception" && echo 'FAIL' || echo 'OK' || :
sleep 1
clickhouse-client -q "SELECT 'add other vector index on vector1 column';"
clickhouse-client -q "ALTER TABLE test_multiple_vector ADD VECTOR INDEX v1_1 vector1 TYPE MSTG;" 2>&1 | grep -q "DB::Exception: Cannot add vector index v1_1: this column already has a vector index definition." && echo 'OK' || echo 'FAIL' || :
sleep 1
clickhouse-client -q "SELECT 'add a vector index on vector2 column';"
clickhouse-client -q "ALTER TABLE test_multiple_vector ADD VECTOR INDEX v2 vector2 TYPE MSTG;" 2>&1 | grep -q "DB::Exception: Cannot add vector index v2: one of the other columns already has a vector index definition." && echo 'OK' || echo 'FAIL' || :
sleep 1
clickhouse-client -q "SELECT 'create other vector index on vector1 column';"
clickhouse-client -q "CREATE VECTOR INDEX v1_2 ON test_multiple_vector vector1 TYPE FLAT;" 2>&1 | grep -q "DB::Exception: Cannot add vector index v1_2: this column already has a vector index definition." && echo 'OK' || echo 'FAIL' || :
sleep 1
clickhouse-client -q "SELECT 'create a vector index on vector2 column';"
clickhouse-client -q "CREATE VECTOR INDEX v2_2 ON test_multiple_vector vector2 TYPE FLAT;" 2>&1 | grep -q "DB::Exception: Cannot add vector index v2_2: one of the other columns already has a vector index definition." && echo 'OK' || echo 'FAIL' || :
sleep 1
clickhouse-client -q "SELECT table, name, type FROM system.vector_indices WHERE table='test_multiple_vector';"
clickhouse-client -q "DROP TABLE IF EXISTS test_multiple_vector;"

# test create single vector column table with single vector index
clickhouse-client -q "DROP TABLE IF EXISTS test_single_vector_table_with_single_index;"
clickhouse-client -q "SELECT 'test create single vector column table with single vector index';"
clickhouse-client -q "CREATE TABLE test_single_vector_table_with_single_index(id UInt32, vector Array(Float32), VECTOR INDEX v1 vector TYPE HNSWFLAT, CONSTRAINT vector_len CHECK length(vector) = 3)engine = MergeTree PRIMARY KEY id;" \
    2>&1 | grep -q "DB::Exception" && echo 'FAIL' || echo 'OK' || :
sleep 1
clickhouse-client -q "SELECT table, name, type FROM system.vector_indices WHERE table='test_single_vector_table_with_single_index';"
clickhouse-client -q "DROP TABLE IF EXISTS test_single_vector_table_with_single_index;"

# test create single vector column table with multiple vector indices
clickhouse-client -q "DROP TABLE IF EXISTS test_single_vector_table_with_multiple_indices;"
clickhouse-client -q "SELECT 'test create single vector column table with multiple vector indices';"
clickhouse-client -q "CREATE TABLE test_single_vector_table_with_multiple_indices(id UInt32, vector Array(Float32), VECTOR INDEX v1 vector TYPE HNSWFLAT, VECTOR INDEX v2 vector TYPE MSTG, CONSTRAINT vector_len CHECK length(vector) = 3)engine = MergeTree PRIMARY KEY id;" \
    2>&1 | grep -q "DB::Exception: Cannot create vector index v2: only one vector index can be created for this table." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "DROP TABLE IF EXISTS test_single_vector_table_with_multiple_indices;"

# test create multiple vector columns table with single vector index
clickhouse-client -q "DROP TABLE IF EXISTS test_multiple_vector_table_with_single_index;"
clickhouse-client -q "SELECT 'test create multiple vector columns table with single vector index';"
clickhouse-client -q "CREATE TABLE test_multiple_vector_table_with_single_index(id UInt32, vector1 Array(Float32), vector2 Array(Float32), VECTOR INDEX v1 vector1 TYPE HNSWFLAT, CONSTRAINT vector1_len CHECK length(vector1) = 3, CONSTRAINT vector2_len CHECK length(vector2) = 3)engine = MergeTree PRIMARY KEY id;" \
    2>&1 | grep -q "DB::Exception" && echo 'FAIL' || echo 'OK' || :
sleep 1
clickhouse-client -q "DROP TABLE IF EXISTS test_multiple_vector_table_with_single_index;"

# test create multiple vector columns table with multiple vector indices
clickhouse-client -q "DROP TABLE IF EXISTS test_multiple_vector_table_with_multiple_indices;"
clickhouse-client -q "SELECT 'test create multiple vector columns table with multiple vector indices';"
clickhouse-client -q "CREATE TABLE test_multiple_vector_table_with_multiple_indices(id UInt32, vector1 Array(Float32), vector2 Array(Float32), VECTOR INDEX v1 vector1 TYPE HNSWFLAT, VECTOR INDEX v2 vector2 TYPE HNSWFLAT, CONSTRAINT vector1_len CHECK length(vector1) = 3, CONSTRAINT vector2_len CHECK length(vector2) = 3)engine = MergeTree PRIMARY KEY id;" \
    2>&1 | grep -q "DB::Exception: Cannot create vector index v2: only one vector index can be created for this table." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "DROP TABLE IF EXISTS test_multiple_vector_table_with_multiple_indices;"

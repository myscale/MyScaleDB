#!/usr/bin/env bash
# Tags: no-parallel, no-tsan, no-asan, no-msan, no-ubsan
# no sanitizer tests because they are logical error, will abort the server

# case1: Syntax error in `CREATE TABLE` query
clickhouse client -q "DROP TABLE IF EXISTS t_check_syntax;"

clickhouse client -q "CREATE TABLE t_check_syntax(id UInt32, vector1 Array(Float32), vector2 Array(Float32), CONSTRAINT vector_len_1 CHECK length(vector1) = 3, CONSTRAINT vector_len_2 CHECK length(vector2) = 3, VECTOR index_name vector1 TYPE MSTG) ENGINE = MergeTree ORDER BY id;" \
    2>&1 | grep -q "DB::Exception: Syntax error: failed at position 208 ('vector1')" && echo 'OK' || echo 'FAIL' || :

clickhouse client -q "CREATE TABLE t_check_syntax(id UInt32, vector1 Array(Float32), vector2 Array(Float32), CONSTRAINT vector_len_1 CHECK length(vector1) = 3, CONSTRAINT vector_len_2 CHECK length(vector2) = 3, INDEX index_name vector1 TYPE MSTG) ENGINE = MergeTree ORDER BY id;" \
    2>&1 | grep -q "DB::Exception: Unknown Index type 'mstg'. Available index types:" && echo 'OK' || echo 'FAIL' || :

clickhouse client -q "CREATE TABLE t_check_syntax(id UInt32, vector1 Array(Float32), vector2 Array(Float32), CONSTRAINT vector_len_1 CHECK length(vector1) = 3, CONSTRAINT vector_len_2 CHECK length(vector2) = 3, VECTOR INDEX index_name vector1 TYPE) ENGINE = MergeTree ORDER BY id;" \
    2>&1 | grep -q "DB::Exception: Syntax error: failed at position 226 (')'): )" && echo 'OK' || echo 'FAIL' || :

clickhouse client -q "CREATE TABLE t_check_syntax(id UInt32, vector1 Array(Float32), vector2 Array(Float32), CONSTRAINT vector_len_1 CHECK length(vector1) = 3, CONSTRAINT vector_len_2 CHECK length(vector2) = 3, VECTOR INDEX index_name vector1 TYPE ABC) ENGINE = MergeTree ORDER BY id;" \
    2>&1 | grep -q "DB::Exception: SearchIndexException: Error(BAD_ARGUMENTS): Unknown index type for Float32 Vector: ABC." && echo 'OK' || echo 'FAIL' || :

clickhouse client -q "CREATE TABLE t_check_syntax(id UInt32, vector1 Array(Float32), vector2 Array(Float32), CONSTRAINT vector_len_1 CHECK length(vector1) = 3, CONSTRAINT vector_len_2 CHECK length(vector2) = 3, VECTOR INDEX index_name vector1 TYPE DEFAULT('test=test')) ENGINE = MergeTree ORDER BY id;" \
    2>&1 | grep -q "DB::Exception: MSTG doesn't support index parameter: \`test\`, valid parameters is \[alpha,metric_type\]." && echo 'OK' || echo 'FAIL' || :

clickhouse client -q "CREATE TABLE t_check_syntax(id UInt32, vector1 Array(Float32), vector2 Array(Float32), CONSTRAINT vector_len_1 CHECK length(vector1) = 3, CONSTRAINT vector_len_2 CHECK length(vector2) = 3, VECTOR INDEX index_name vector1, VECTOR INDEX index_name vector2) ENGINE = MergeTree ORDER BY id;" \
    2>&1 | grep -q "DB::Exception: Vector index with name \`index_name\` already exists. (LOGICAL_ERROR)" && echo 'OK' || echo 'FAIL' || :

# case2: Syntax error in `ALTER TABLE... ADD VECTOR INDEX` query
clickhouse client -q "DROP TABLE IF EXISTS t_check_syntax;"
clickhouse client -q "CREATE TABLE t_check_syntax(id UInt32, vector1 Array(Float32), vector2 Array(Float32), CONSTRAINT vector_len_1 CHECK length(vector1) = 3, CONSTRAINT vector_len_2 CHECK length(vector2) = 3) ENGINE = MergeTree ORDER BY id SETTINGS vector_index_parameter_check = 1;"

clickhouse client -q "ALTER TABLE t_check_syntax ADD VECTOR index_name vector1 TYPE MSTG" \
    2>&1 | grep -q "DB::Exception: Syntax error: failed at position 39 ('index_name'): index_name vector1 TYPE MSTG. Expected INDEX. (SYNTAX_ERROR)" && echo 'OK' || echo 'FAIL' || :

clickhouse client -q "ALTER TABLE t_check_syntax ADD INDEX index_name vector1 TYPE MSTG" \
    2>&1 | grep -q "DB::Exception: Unknown Index type 'mstg'. Available index types:" && echo 'OK' || echo 'FAIL' || :

clickhouse client -q "ALTER TABLE t_check_syntax ADD VECTOR INDEX index_name vector1 TYPE" \
    2>&1 | grep -q "DB::Exception: Syntax error: failed at position 68 (end of query): . Expected one of: expression with optional parameters, function, function name, compound identifier, identifier. (SYNTAX_ERROR)" && echo 'OK' || echo 'FAIL' || :

clickhouse client -q "ALTER TABLE t_check_syntax ADD VECTOR INDEX index_name vector1 TYPE ABC()" \
    2>&1 | grep -q "DB::Exception: SearchIndexException: Error(BAD_ARGUMENTS): Unknown index type for Float32 Vector: ABC. (STD_EXCEPTION)" && echo 'OK' || echo 'FAIL' || :

clickhouse client -q "ALTER TABLE t_check_syntax ADD VECTOR INDEX index_name vector1 TYPE DEFAULT('test=test')" \
    2>&1 | grep -q "DB::Exception: MSTG doesn't support index parameter: \`test\`, valid parameters is \[alpha,metric_type\]. (BAD_ARGUMENTS)" && echo 'OK' || echo 'FAIL' || :

clickhouse client -q "ALTER TABLE t_check_syntax ADD VECTOR INDEX index_name vector2"
clickhouse client -q "ALTER TABLE t_check_syntax ADD VECTOR INDEX index_name vector1" \
    2>&1 | grep -q "DB::Exception: Cannot add vector index index_name: this name is used. (BAD_ARGUMENTS)" && echo 'OK' || echo 'FAIL' || :

# case3: Syntax error in `CREATE VECTOR INDEX ...` query
clickhouse client -q "DROP VECTOR INDEX index_name ON t_check_syntax;"
# ck24.8 doesn't support `CREATE INDEX without type` syntax
# clickhouse client -q "CREATE INDEX index_name ON t_check_syntax vector1" \
#     2>&1 | grep -q "DB::Exception: Syntax error: failed at position 50 (end of query)" && echo 'OK' || echo 'FAIL' || :

clickhouse client -q "CREATE VECTOR index_name ON t_check_syntax vector1" \
    2>&1 | grep -q "DB::Exception: Syntax error: failed at position 15 ('index_name'): index_name ON t_check_syntax vector1. Expected INDEX. (SYNTAX_ERROR)" && echo 'OK' || echo 'FAIL' || :

clickhouse client -q "CREATE VECTOR INDEX index_name ON t_check_syntax vector1 TYPE" \
    2>&1 | grep -q "DB::Exception: Syntax error: failed at position 62 (end of query): . Expected one of: expression with optional parameters, function, function name, compound identifier, identifier. (SYNTAX_ERROR)" && echo 'OK' || echo 'FAIL' || :

clickhouse client -q "CREATE VECTOR INDEX index_name ON t_check_syntax vector1 TYPE ABC()" \
    2>&1 | grep -q "DB::Exception: SearchIndexException: Error(BAD_ARGUMENTS): Unknown index type for Float32 Vector: ABC. (STD_EXCEPTION)" && echo 'OK' || echo 'FAIL' || :

clickhouse client -q "CREATE VECTOR INDEX index_name ON t_check_syntax vector1 TYPE DEFAULT('test=test')" \
    2>&1 | grep -q "DB::Exception: MSTG doesn't support index parameter: \`test\`, valid parameters is \[alpha,metric_type\]. (BAD_ARGUMENTS)" && echo 'OK' || echo 'FAIL' || :

clickhouse client -q "CREATE VECTOR INDEX index_name ON t_check_syntax vector2"
clickhouse client -q "CREATE VECTOR INDEX index_name ON t_check_syntax vector1 TYPE DEFAULT" \
    2>&1 | grep -q "DB::Exception: Cannot add vector index index_name: this name is used. (BAD_ARGUMENTS)" && echo 'OK' || echo 'FAIL' || :

clickhouse client -q "DROP TABLE IF EXISTS t_check_syntax;"

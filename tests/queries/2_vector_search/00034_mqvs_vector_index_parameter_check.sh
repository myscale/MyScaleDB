#!/usr/bin/env bash
# Tags: no-parallel

# reference: https://moqi.quip.com/U2xhAJL2YmRI MYSCALE_OSS_DELETE_LINE

# Case1. Can't create table with vector index if no constraint
clickhouse-client -q "SELECT 'Case1';"
clickhouse-client -q "DROP TABLE IF EXISTS table_with_vector_index_no_constraint;"
clickhouse-client -q "CREATE TABLE table_with_vector_index_no_constraint(id UInt32, vector Array(Float32), VECTOR INDEX index_name vector TYPE MSTG('metric_type=Cosine')) ENGINE = MergeTree ORDER BY id;" 2>&1 | grep -q "DB::Exception: Cannot create table with column 'vector' which type is 'Array(Float32)' because the constraint information was not defined during the creation of a vector index for the column." && echo 'OK' || echo 'FAIL' || :
# clickhouse-client -q "SELECT name FROM system.tables WHERE (database = 'default') AND (name = 'table_with_vector_index_no_constraint');"
clickhouse-client -q "DROP TABLE IF EXISTS table_with_vector_index_no_constraint;"

# Case2. When creating a table with vector index, we should check whether vector element type is Float32 or not.
clickhouse-client -q "SELECT 'Case2';"
clickhouse-client -q "DROP TABLE IF EXISTS table_with_vector_index_float64;"
clickhouse-client -q "CREATE TABLE table_with_vector_index_float64 (id UInt32, vector Array(Float64), VECTOR INDEX MSTG_NAME vector TYPE mstg('metric_type=Cosine'), CONSTRAINT check_length CHECK length(vector) = 512) ENGINE = MergeTree ORDER BY id" 2>&1 | grep -q "DB::Exception: The element type inside the array must be \`Float32\`." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "DROP TABLE IF EXISTS table_with_vector_index_float64;"

# Case3. When using alter statement to create a vector index, we should check whether vector element type is Float32 or not.
clickhouse-client -q "SELECT 'Case3';"
clickhouse-client -q "DROP TABLE IF EXISTS table_vector_type_float64;"
clickhouse-client -q "CREATE TABLE table_vector_type_float64 (id UInt32, vector Array(Float64), CONSTRAINT check_length CHECK length(vector) = 512) ENGINE = MergeTree ORDER BY id"
clickhouse-client -q "ALTER TABLE table_vector_type_float64 ADD VECTOR INDEX MSTG_NAME vector TYPE mstg('metric_type=Cosine')" 2>&1 | grep -q "DB::Exception: The element type inside the array must be \`Float32\`." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "DROP TABLE IF EXISTS table_vector_type_float64;"

# Case4. Brutefore can't run when vector type is not Float32.
clickhouse-client -q "SELECT 'Case4';"
clickhouse-client -q "DROP TABLE IF EXISTS table_vector_type_float64;"
clickhouse-client -q "CREATE TABLE table_vector_type_float64 (id UInt32, vector Array(Float64), CONSTRAINT check_length CHECK length(vector) = 3) ENGINE = MergeTree ORDER BY id"
clickhouse-client -q "INSERT INTO table_vector_type_float64 SELECT number, [number,number,number] FROM numbers(1000);"
clickhouse-client -q "SELECT id, distance(vector, [1,2,3]) as dis from table_vector_type_float64 order by dis asc limit 10;" 2>&1 | grep -q "DB::Exception: The element type inside the array must be \`Float32\`." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "DROP TABLE IF EXISTS table_vector_type_float64;"

# Case5.  Checking vector index parameter when create table with vector index.
clickhouse-client -q "SELECT 'Case5';"
clickhouse-client -q "DROP TABLE IF EXISTS table_with_check"
clickhouse-client -q "CREATE TABLE table_with_check (id UInt32, vector Array(Float32), CONSTRAINT check_length CHECK length(vector) = 16) ENGINE = MergeTree ORDER BY id SETTINGS vector_index_parameter_check=1"
clickhouse-client -q "INSERT INTO table_with_check SELECT number, [number,number,number,number,number,number,number,number,number,number,number,number,number,number,number,number] FROM numbers(1000);"
## Case5.1 IVFPQ: ncentroids ranges  from 1 to 1048576
clickhouse-client -q "SELECT 'Case5.1';"
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfpq('metric_type=Cosine', 'ncentroids=3.9')" 2>&1 | grep -q "DB::Exception: IVFPQ expects an integer value for parameter: \`ncentroids\`, but got \`3.9\`." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfpq('metric_type=Cosine', 'ncentroids=hello')" 2>&1 | grep -q "DB::Exception: IVFPQ expects an integer value for parameter: \`ncentroids\`, but got \`hello\`." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfpq('metric_type=Cosine', 'ncentroids=0')" 2>&1 | grep -q "DB::Exception: IVFPQ parameter \`ncentroids\` range needs to be 1~1048576." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfpq('metric_type=Cosine', 'ncentroids=1048577')" 2>&1 | grep -q "DB::Exception: IVFPQ parameter \`ncentroids\` range needs to be 1~1048576." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfpq('metric_type=Cosine', 'ncentroids=1048')"
clickhouse-client -q "SELECT table, name, expr FROM system.vector_indices WHERE database = currentDatabase() and table = 'table_with_check';"
## Case5.2 IVFPQ: M ranges from 1~dim, dim%M==0
clickhouse-client -q "SELECT 'Case5.2';"
clickhouse-client -q "ALTER TABLE table_with_check DROP VECTOR INDEX index_name"
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfpq('metric_type=Cosine', 'M=0')" 2>&1 | grep -q "DB::Exception: IVFPQ needs \`dim\`%\`M\`==0, \`dim\`!=0, \`M\`!=0." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfpq('metric_type=Cosine', 'M=5')" 2>&1 | grep -q "DB::Exception: IVFPQ needs \`dim\`%\`M\`==0, \`dim\`!=0, \`M\`!=0." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfpq('metric_type=Cosine', 'M=8')"
clickhouse-client -q "SELECT table, name, expr FROM system.vector_indices WHERE database = currentDatabase() and table = 'table_with_check';"
clickhouse-client -q "ALTER TABLE table_with_check DROP VECTOR INDEX index_name"
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfpq('metric_type=Cosine', 'M=17')" 2>&1 | grep -q "DB::Exception: IVFPQ needs \`dim\`%\`M\`==0, \`dim\`!=0, \`M\`!=0." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfpq('metric_type=Cosine', 'M=hello')" 2>&1 | grep -q "DB::Exception: IVFPQ expects an integer value for parameter: \`M\`, but got \`hello\`." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfpq('metric_type=Cosine', 'M=9.7')" 2>&1 | grep -q "DB::Exception: IVFPQ expects an integer value for parameter: \`M\`, but got \`9.7\`." && echo 'OK' || echo 'FAIL' || :
## Case5.3 IVFPQ: bit_size ranges from 2 to 12
clickhouse-client -q "SELECT 'Case5.3';"
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfpq('metric_type=Cosine', 'bit_size=1')" 2>&1 | grep -q "DB::Exception: IVFPQ parameter \`bit_size\` range needs to be 2~12." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfpq('metric_type=Cosine', 'bit_size=13')" 2>&1 | grep -q "DB::Exception: IVFPQ parameter \`bit_size\` range needs to be 2~12." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfpq('metric_type=Cosine', 'bit_size=hello')" 2>&1 | grep -q "DB::Exception: IVFPQ expects an integer value for parameter: \`bit_size\`, but got \`hello\`." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfpq('metric_type=Cosine', 'bit_size=3.4')" 2>&1 | grep -q "DB::Exception: IVFPQ expects an integer value for parameter: \`bit_size\`, but got \`3.4\`." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfpq('metric_type=Cosine', 'bit_size=10')"
clickhouse-client -q "SELECT table, name, expr FROM system.vector_indices WHERE database = currentDatabase() and table = 'table_with_check';"
clickhouse-client -q "ALTER TABLE table_with_check DROP VECTOR INDEX index_name"
## Case5.4 IVFSQ: bit_size candidates: 8bit, 6bit, 4bit, 8bit_uniform, 8bit_direct, 4bit_uniform, QT_fp16
clickhouse-client -q "SELECT 'Case5.4';"
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfsq('metric_type=Cosine', 'bit_size=8Bit')" 2>&1 | grep -q "DB::Exception: IVFSQ parameter \`bit_size\` should be one of \[4bit, 6bit, 8bit, 8bit_uniform, 8bit_direct, 4bit_uniform, QT_fp16\]." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfsq('metric_type=Cosine', 'bit_size=8bit')"
clickhouse-client -q "SELECT table, name, expr FROM system.vector_indices WHERE database = currentDatabase() and table = 'table_with_check';"
clickhouse-client -q "ALTER TABLE table_with_check DROP VECTOR INDEX index_name"
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfsq('metric_type=Cosine', 'bit_size=6bit')"
clickhouse-client -q "SELECT table, name, expr FROM system.vector_indices WHERE database = currentDatabase() and table = 'table_with_check';"
clickhouse-client -q "ALTER TABLE table_with_check DROP VECTOR INDEX index_name"
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfsq('metric_type=Cosine', 'bit_size=4bit')"
clickhouse-client -q "SELECT table, name, expr FROM system.vector_indices WHERE database = currentDatabase() and table = 'table_with_check';"
clickhouse-client -q "ALTER TABLE table_with_check DROP VECTOR INDEX index_name"
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfsq('metric_type=Cosine', 'bit_size=8bit_uniform')"
clickhouse-client -q "SELECT table, name, expr FROM system.vector_indices WHERE database = currentDatabase() and table = 'table_with_check';"
clickhouse-client -q "ALTER TABLE table_with_check DROP VECTOR INDEX index_name"
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfsq('metric_type=Cosine', 'bit_size=8bit_direct')"
clickhouse-client -q "SELECT table, name, expr FROM system.vector_indices WHERE database = currentDatabase() and table = 'table_with_check';"
clickhouse-client -q "ALTER TABLE table_with_check DROP VECTOR INDEX index_name"
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfsq('metric_type=Cosine', 'bit_size=4bit_uniform')"
clickhouse-client -q "SELECT table, name, expr FROM system.vector_indices WHERE database = currentDatabase() and table = 'table_with_check';"
clickhouse-client -q "ALTER TABLE table_with_check DROP VECTOR INDEX index_name"
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfsq('metric_type=Cosine', 'bit_size=QT_fp16')"
clickhouse-client -q "SELECT table, name, expr FROM system.vector_indices WHERE database = currentDatabase() and table = 'table_with_check';"
clickhouse-client -q "ALTER TABLE table_with_check DROP VECTOR INDEX index_name"
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfsq('metric_type=Cosine', 'bit_size=8')" 2>&1 | grep -q "DB::Exception: IVFSQ parameter \`bit_size\` should be one of \[4bit, 6bit, 8bit, 8bit_uniform, 8bit_direct, 4bit_uniform, QT_fp16\]." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE ivfsq('metric_type=Cosine', 'bit_size=8.901')" 2>&1 | grep -q "DB::Exception: IVFSQ parameter \`bit_size\` should be one of \[4bit, 6bit, 8bit, 8bit_uniform, 8bit_direct, 4bit_uniform, QT_fp16\]." && echo 'OK' || echo 'FAIL' || :
## Case5.5 HNSWFLAT: m ranges from 8~128
clickhouse-client -q "SELECT 'Case5.5';"
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE hnswflat('metric_type=Cosine', 'm=7')" 2>&1 | grep -q "DB::Exception: HNSWFLAT parameter \`m\` range needs to be 8~128." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE hnswflat('metric_type=Cosine', 'm=129')" 2>&1 | grep -q "DB::Exception: HNSWFLAT parameter \`m\` range needs to be 8~128." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE hnswflat('metric_type=Cosine', 'ef_c=1')" 2>&1 | grep -q "DB::Exception: HNSWFLAT parameter \`ef_c\` range needs to be 16~1024." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE hnswflat('metric_type=Cosine', 'm=16')"
clickhouse-client -q "SELECT table, name, expr FROM system.vector_indices WHERE database = currentDatabase() and table = 'table_with_check';"
clickhouse-client -q "ALTER TABLE table_with_check DROP VECTOR INDEX index_name"
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE hnswflat('metric_type=Cosine', 'disk_mode=1')" 2>&1 | grep -q "DB::Exception: HNSWFLAT doesn't support index parameter: \`disk_mode\`, valid parameters is \[ef_c,ef_s,m,metric_type\]." && echo 'OK' || echo 'FAIL' || :
## Case5.6 MSTG doesn't allow any vector index create parameter
clickhouse-client -q "SELECT 'Case5.6';"
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX index_name vector TYPE mstg('metric_type=Cosine', 'disk_mode=1')" 2>&1 | grep -q " MSTG doesn't support index parameter: \`disk_mode\`, valid parameters is \[alpha,metric_type\]." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "DROP TABLE IF EXISTS table_with_check"


# Case6.  Not checking vector index parameter when create table with vector index.
clickhouse-client -q "SELECT 'Case6';"
clickhouse-client -q "DROP TABLE IF EXISTS table_no_check"
clickhouse-client -q "CREATE TABLE table_no_check (id UInt32, vector Array(Float32), CONSTRAINT check_length CHECK length(vector) = 16) ENGINE = MergeTree ORDER BY id SETTINGS vector_index_parameter_check=0"
clickhouse-client -q "INSERT INTO table_no_check SELECT number, [number,number,number,number,number,number,number,number,number,number,number,number,number,number,number,number] FROM numbers(1000);"
clickhouse-client -q "ALTER TABLE table_no_check ADD VECTOR INDEX index_name vector TYPE mstg('metric_type=Cosine', 'disk_mode=1')"
clickhouse-client -q "SYSTEM WAIT BUILDING VECTOR INDICES table_no_check"
clickhouse-client -q "SELECT table, name, expr FROM system.vector_indices WHERE database = currentDatabase() and table = 'table_no_check';"
clickhouse-client -q "ALTER TABLE table_no_check DROP VECTOR INDEX index_name"
clickhouse-client -q "ALTER TABLE table_no_check ADD VECTOR INDEX index_name vector TYPE hnswflat('metric_type=Cosine', 'm=7')"
clickhouse-client -q "SELECT table, name, expr FROM system.vector_indices WHERE database = currentDatabase() and table = 'table_no_check';"
clickhouse-client -q "ALTER TABLE table_no_check DROP VECTOR INDEX index_name"
clickhouse-client -q "ALTER TABLE table_no_check ADD VECTOR INDEX index_name vector TYPE ivfsq('metric_type=Cosine', 'bit_size=8Bit')"
clickhouse-client -q "SELECT table, name, expr FROM system.vector_indices WHERE database = currentDatabase() and table = 'table_no_check';"
clickhouse-client -q "ALTER TABLE table_no_check DROP VECTOR INDEX index_name"
clickhouse-client -q "DROP TABLE IF EXISTS table_no_check"


# Case6.1. Checking search parameter when execute vector search.
clickhouse-client -q "SELECT 'Case6.1';"
clickhouse-client -q "DROP TABLE IF EXISTS table_with_check"
clickhouse-client -q "CREATE TABLE table_with_check (id UInt32, vector Array(Float32), CONSTRAINT check_length CHECK length(vector) = 16) ENGINE = MergeTree ORDER BY id SETTINGS vector_index_parameter_check=1"
clickhouse-client -q "INSERT INTO table_with_check SELECT number, [number,number,number,number,number,number,number,number,number,number,number,number,number,number,number,number] FROM numbers(1000);"
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX mstg_name vector TYPE mstg('metric_type=Cosine')"
clickhouse-client -q "SYSTEM WAIT BUILDING VECTOR INDICES table_with_check"
clickhouse-client -q "SELECT table, name, expr FROM system.vector_indices WHERE database = currentDatabase() and table = 'table_with_check';"
clickhouse-client -q "SELECT count(*) from (SELECT id, distance('alpha=3.7')(vector, [1.,2.,3.,4.,5.,6.,7.,8.,9.,10.,11.,12.,13.,14.,15.,16.]) as dis from table_with_check order by dis asc limit 1) as temp;"
clickhouse-client -q "SELECT id, distance('alpha=4.2')(vector, [1.,2.,3.,4.,5.,6.,7.,8.,9.,10.,11.,12.,13.,14.,15.,16.]) as dis from table_with_check order by dis asc limit 1;" 2>&1 | grep -q "DB::Exception: Value for parameter \`alpha\` range needs to be 1~4." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "ALTER TABLE table_with_check DROP VECTOR INDEX mstg_name"
clickhouse-client -q "ALTER TABLE table_with_check ADD VECTOR INDEX mstg_name vector TYPE hnswflat('metric_type=Cosine')"
clickhouse-client -q "SELECT table, name, expr FROM system.vector_indices WHERE database = currentDatabase() and table = 'table_with_check';"
clickhouse-client -q "SELECT id, distance('ef_s=15')(vector, [1.,2.,3.,4.,5.,6.,7.,8.,9.,10.,11.,12.,13.,14.,15.,16.]) as dis from table_with_check order by dis asc limit 1;" 2>&1 | grep -q "DB::Exception: Value for parameter \`ef_s\` range needs to be 16~1024." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "DROP TABLE IF EXISTS table_with_check"


# Case6.2. Not check search parameter when execute vector search.
clickhouse-client -q "SELECT 'Case6.2';"
clickhouse-client -q "DROP TABLE IF EXISTS table_no_check"
clickhouse-client -q "CREATE TABLE table_no_check (id UInt32, vector Array(Float32), CONSTRAINT check_length CHECK length(vector) = 16) ENGINE = MergeTree ORDER BY id SETTINGS vector_index_parameter_check=0"
clickhouse-client -q "INSERT INTO table_no_check SELECT number, [number,number,number,number,number,number,number,number,number,number,number,number,number,number,number,number] FROM numbers(1000);"
clickhouse-client -q "ALTER TABLE table_no_check ADD VECTOR INDEX mstg_name vector TYPE mstg('metric_type=Cosine')"
clickhouse-client -q "SYSTEM WAIT BUILDING VECTOR INDICES table_no_check"
clickhouse-client -q "SELECT table, name, expr FROM system.vector_indices WHERE database = currentDatabase() and table = 'table_no_check';"
clickhouse-client -q "SELECT id, distance('alpha=4.2')(vector, [1.,2.,3.,4.,5.,6.,7.,8.,9.,10.,11.,12.,13.,14.,15.,16.]) as dis from table_no_check order by dis asc limit 1;"  2>&1 | grep -q "DB::Exception: VectorIndex: Error in VI_Search, Error(BAD_ARGUMENTS): 'alpha >= 1.0 && alpha <= 4.0' failed: alpha should be between 1.0 and 4.0" && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "ALTER TABLE table_no_check DROP VECTOR INDEX mstg_name"
clickhouse-client -q "ALTER TABLE table_no_check ADD VECTOR INDEX mstg_name vector TYPE hnswflat('metric_type=Cosine')" 
clickhouse-client -q "SELECT table, name, expr FROM system.vector_indices WHERE database = currentDatabase() and table = 'table_no_check';"
clickhouse-client -q "SELECT count(*) from (SELECT id, distance('ef_s=15')(vector, [1.,2.,3.,4.,5.,6.,7.,8.,9.,10.,11.,12.,13.,14.,15.,16.]) as dis from table_no_check order by dis asc limit 1) as temp SETTINGS enable_brute_force_vector_search=1;"
clickhouse-client -q "DROP TABLE IF EXISTS table_no_check"


# Case7. Checking Binary vector search Parameter
clickhouse-client -q "SELECT 'Case7';"
clickhouse-client -q "SELECT 'TEST Binary Vector Search'"
clickhouse-client -q "DROP TABLE IF EXISTS table_with_binary_vector_check"
clickhouse-client -q "CREATE TABLE table_with_binary_vector_check (id UInt32, vector FixedString(4)) ENGINE = MergeTree ORDER BY id SETTINGS binary_vector_search_metric_type='Hamming', vector_index_parameter_check=1;"
clickhouse-client -q "INSERT INTO table_with_binary_vector_check SELECT number, char(number, number, number, number) FROM numbers(100);"
## Case7.1 BinaryFLAT has only one parameter(metric_type)
clickhouse-client -q "SELECT 'Case7.1';"
clickhouse-client -q "ALTER TABLE table_with_binary_vector_check ADD VECTOR INDEX vec_ind_flat vector TYPE BinaryFLAT('metric_type=Cosine');" 2>&1 | grep -q "DB::Exception: BINARYFLAT parameter \`metric_type\` should be one of " && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "ALTER TABLE table_with_binary_vector_check ADD VECTOR INDEX vec_ind_flat vector TYPE BinaryFLAT('metric_type=Hamming', 'ncentroids=hello');" 2>&1 | grep -q "DB::Exception: BINARYFLAT doesn't support index parameter: \`ncentroids\`, valid parameters is \[metric_type\]." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "ALTER TABLE table_with_binary_vector_check ADD VECTOR INDEX vec_ind_flat vector TYPE BinaryFLAT('metric_type=Hamming');"
clickhouse-client -q "SYSTEM WAIT BUILDING VECTOR INDICES table_with_binary_vector_check"
clickhouse-client -q "SELECT table, name, expr FROM system.vector_indices WHERE database = currentDatabase() and table = 'table_with_binary_vector_check';"
clickhouse-client -q "ALTER TABLE table_with_binary_vector_check DROP VECTOR INDEX vec_ind_flat;"

clickhouse-client -q "DROP TABLE IF EXISTS table_with_binary_vector_check"

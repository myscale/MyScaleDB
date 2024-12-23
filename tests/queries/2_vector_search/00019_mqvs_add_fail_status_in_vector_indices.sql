-- Tags: no-parallel

DROP TABLE IF EXISTS test_success_vector;
CREATE TABLE test_success_vector(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) engine MergeTree primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=100, vector_index_parameter_check=0;
INSERT INTO test_success_vector SELECT number, [number, number, number] FROM numbers(2100);
ALTER TABLE test_success_vector ADD VECTOR INDEX v1_success vector TYPE HNSWFLAT;

DROP TABLE IF EXISTS test_fail_vector;
CREATE TABLE test_fail_vector(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) engine MergeTree primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=100, vector_index_parameter_check=0;
INSERT INTO test_fail_vector SELECT number, [number, number, number] FROM numbers(2100);
-- Unsupported parameter: metric_type = unknown
ALTER TABLE test_fail_vector ADD VECTOR INDEX v1_fail vector TYPE HNSWSQ('metric_type = unknown', 'ef_c=256');

DROP TABLE IF EXISTS test_fail_vector_2;
CREATE TABLE test_fail_vector_2(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) engine MergeTree primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=100, vector_index_parameter_check=0;
INSERT INTO test_fail_vector_2 SELECT number, [number, number, number] FROM numbers(2100);
-- Unsupported parameter: meric=IP
ALTER TABLE test_fail_vector_2 ADD VECTOR INDEX vindex vector TYPE IVFFLAT('metric=IP', 'ncentroids=5000');

SYSTEM WAIT BUILDING VECTOR INDICES test_fail_vector_2; -- { serverError INVALID_VECTOR_INDEX }
SYSTEM WAIT BUILDING VECTOR INDICES test_success_vector;

select table, name, expr, status, latest_failed_part, latest_fail_reason from system.vector_indices where database = currentDatabase() order by table;

DROP TABLE test_fail_vector_2;
DROP TABLE test_fail_vector;
DROP TABLE test_success_vector;

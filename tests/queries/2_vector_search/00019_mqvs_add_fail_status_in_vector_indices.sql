DROP TABLE IF EXISTS test_success_vector;
CREATE TABLE test_success_vector(id Float32, vector FixedArray(Float32, 3)) engine MergeTree primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=100;
INSERT INTO test_success_vector SELECT number, [number, number, number] FROM numbers(2100);
ALTER TABLE test_success_vector ADD VECTOR INDEX v1_success vector TYPE HNSWFLAT;

DROP TABLE IF EXISTS test_fail_vector;
CREATE TABLE test_fail_vector(id Float32, vector FixedArray(Float32, 3)) engine MergeTree primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=100;
INSERT INTO test_fail_vector SELECT number, [number, number, number] FROM numbers(2100);
-- Unsupported parameter: metric_type = cosine
ALTER TABLE test_fail_vector ADD VECTOR INDEX v1_fail vector TYPE HNSWSQ('metric_type = cosine', 'ef_c=256');

DROP TABLE IF EXISTS test_fail_vector_2;
CREATE TABLE test_fail_vector_2(id Float32, vector FixedArray(Float32, 3)) engine MergeTree primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=100;
INSERT INTO test_fail_vector_2 SELECT number, [number, number, number] FROM numbers(2100);
-- Unsupported parameter: meric=IP
ALTER TABLE test_fail_vector_2 ADD VECTOR INDEX vindex vector TYPE IVFFLAT('metric=IP', 'ncentroids=5000');

select sleep(2);

select table, name, expr, status, latest_failed_part, substr(latest_fail_reason, position(latest_fail_reason,'ception') + 8) from system.vector_indices where database = currentDatabase() order by table;

DROP TABLE test_fail_vector_2;
DROP TABLE test_fail_vector;
DROP TABLE test_success_vector;

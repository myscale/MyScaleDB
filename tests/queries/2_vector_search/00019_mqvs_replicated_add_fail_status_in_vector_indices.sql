-- Tags: no-parallel

DROP TABLE IF EXISTS test_replicated_success_vector SYNC;
CREATE TABLE test_replicated_success_vector(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/mqvs_00019/success_vector', 'r1') primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=100, vector_index_parameter_check=0;
ALTER TABLE test_replicated_success_vector ADD VECTOR INDEX v1_success vector TYPE HNSWFLAT;
INSERT INTO test_replicated_success_vector SELECT number, [number, number, number] FROM numbers(2100);

SYSTEM WAIT BUILDING VECTOR INDICES test_replicated_success_vector;

DROP TABLE IF EXISTS test_replicated_fail_vector SYNC;
CREATE TABLE test_replicated_fail_vector(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/mqvs_00019/fail_vector', 'r1') primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=100, vector_index_parameter_check=0;
-- Unsupported parameter: metric_type = unknown
ALTER TABLE test_replicated_fail_vector ADD VECTOR INDEX v1_fail vector TYPE HNSWSQ('metric_type = unknown', 'ef_c=256');
INSERT INTO test_replicated_fail_vector SELECT number, [number, number, number] FROM numbers(2100);

SYSTEM WAIT BUILDING VECTOR INDICES test_replicated_fail_vector; -- { serverError INVALID_VECTOR_INDEX }

DROP TABLE IF EXISTS test_replicated_fail_vector_2 SYNC;
CREATE TABLE test_replicated_fail_vector_2(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/mqvs_00019/fail_vector_2', 'r1') primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=100, vector_index_parameter_check=0;
-- Unsupported parameter: meric=IP
ALTER TABLE test_replicated_fail_vector_2 ADD VECTOR INDEX vindex vector TYPE IVFFLAT('metric=IP', 'ncentroids=5000');
INSERT INTO test_replicated_fail_vector_2 SELECT number, [number, number, number] FROM numbers(2100);

SYSTEM WAIT BUILDING VECTOR INDICES test_replicated_fail_vector_2; -- { serverError INVALID_VECTOR_INDEX }

SELECT table, name, expr, status, latest_failed_part, latest_fail_reason from system.vector_indices where database = currentDatabase() order by table;

DROP TABLE test_replicated_fail_vector_2 SYNC;
DROP TABLE test_replicated_fail_vector SYNC;
DROP TABLE test_replicated_success_vector SYNC;

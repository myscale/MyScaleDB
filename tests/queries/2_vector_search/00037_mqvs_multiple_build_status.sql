-- Tags: no-parallel

SELECT '-- Test build vector index status';
DROP TABLE IF EXISTS test_multi_status SYNC;
CREATE TABLE test_multi_status (id UInt32, data Array(Float32), v2 Array(Float32),
CONSTRAINT check_length CHECK length(data) = 768, CONSTRAINT check_length_v2 CHECK length(v2) = 768)
ENGINE = MergeTree ORDER BY id 
SETTINGS min_bytes_to_build_vector_index = 0, vector_index_parameter_check=0;

INSERT INTO test_multi_status SELECT number as id, arrayMap(x -> (rand() % 10000000) / 10000000.0 * (if(rand() % 2 = 0, 1, -1)), range(768)) as vector, arrayMap(x -> (rand() % 10000000) / 10000000.0 * (if(rand() % 2 = 0, 1, -1)), range(768)) as v2 FROM numbers(1000);

ALTER TABLE test_multi_status ADD VECTOR INDEX idx data TYPE MSTG('unknown=1');
ALTER TABLE test_multi_status ADD VECTOR INDEX idx_v2 v2 TYPE MSTG('unknown=1');

SYSTEM WAIT BUILDING VECTOR INDICES test_multi_status; -- { serverError INVALID_VECTOR_INDEX }
select table, name, expr, status, latest_failed_part, latest_fail_reason from system.vector_indices where database = currentDatabase() and table = 'test_multi_status';

SELECT 'After drop the first vector index idx';
ALTER TABLE test_multi_status DROP VECTOR INDEX idx;

select table, name, expr, status, latest_failed_part, latest_fail_reason from system.vector_indices where database = currentDatabase() and table = 'test_multi_status';

SYSTEM STOP BUILD VECTOR INDICES test_multi_status;
SELECT 'After newly add again the first vector index idx';
ALTER TABLE test_multi_status ADD VECTOR INDEX idx data TYPE MSTG;

SELECT 'InProgress for index idx when build vector indices are stopped';
select table, name, expr, status from system.vector_indices where database = currentDatabase() and table = 'test_multi_status';

SYSTEM START BUILD VECTOR INDICES test_multi_status;
SELECT sleep(3);
SELECT sleep(3);

SELECT 'Built when for index idx build vector indices are started';
select table, name, expr, status from system.vector_indices where database = currentDatabase() and table = 'test_multi_status';

DROP TABLE test_multi_status SYNC;
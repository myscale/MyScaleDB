-- Tags: no-parallel
DROP TABLE IF EXISTS test_172_status SYNC;
CREATE TABLE test_172_status (id UInt32, data Array(Float32), CONSTRAINT check_length CHECK length(data) = 768) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_to_build_vector_index = 0, vector_index_parameter_check=0;

INSERT INTO test_172_status SELECT number as id, arrayMap(x -> (rand() % 10000000) / 10000000.0 * (if(rand() % 2 = 0, 1, -1)), range(768)) as vector FROM numbers(1000);

ALTER TABLE test_172_status ADD VECTOR INDEX idx data TYPE MSTG('unknown=1');

SYSTEM WAIT BUILDING VECTOR INDICES test_172_status; -- { serverError INVALID_VECTOR_INDEX }

select table, name, expr, status, latest_failed_part, latest_fail_reason from system.vector_indices where database = currentDatabase() order by table;

ALTER TABLE test_172_status DROP VECTOR INDEX idx;

SYSTEM STOP merges test_172_status;
ALTER TABLE test_172_status ADD VECTOR INDEX idx data TYPE MSTG;

select table, name, expr, status from system.vector_indices where database = currentDatabase() order by table;

DROP TABLE test_172_status;

DROP TABLE IF EXISTS test_172_replicated_status SYNC;
CREATE TABLE test_172_replicated_status (id UInt32, data Array(Float32), CONSTRAINT check_length CHECK length(data) = 768) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/mqvs_00033/test_172_status', 'r1') ORDER BY id SETTINGS min_bytes_to_build_vector_index = 0, vector_index_parameter_check=0;

INSERT INTO test_172_replicated_status SELECT number as id, arrayMap(x -> (rand() % 10000000) / 10000000.0 * (if(rand() % 2 = 0, 1, -1)), range(768)) as vector FROM numbers(1000);

ALTER TABLE test_172_replicated_status ADD VECTOR INDEX idx data TYPE MSTG('unknown=1');

SYSTEM WAIT BUILDING VECTOR INDICES test_172_replicated_status; -- { serverError INVALID_VECTOR_INDEX }

select table, name, expr, status, latest_failed_part, substr(latest_fail_reason, position(latest_fail_reason,'ception') + 8) from system.vector_indices where database = currentDatabase() order by table;

ALTER TABLE test_172_replicated_status DROP VECTOR INDEX idx;

SYSTEM STOP merges test_172_replicated_status;
ALTER TABLE test_172_replicated_status ADD VECTOR INDEX idx data TYPE MSTG;

select table, name, expr, status from system.vector_indices where database = currentDatabase() order by table;

DROP TABLE test_172_replicated_status;

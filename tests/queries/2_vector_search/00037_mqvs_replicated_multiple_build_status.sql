-- Tags: no-parallel


SELECT '-- Test build vector index status';
DROP TABLE IF EXISTS test_multi_replica_status SYNC;
CREATE TABLE test_multi_replica_status (id UInt32, data Array(Float32), v2 Array(Float32),
CONSTRAINT check_length CHECK length(data) = 768, CONSTRAINT check_length_v2 CHECK length(v2) = 768)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_multi_status', 'r1') ORDER BY id 
SETTINGS min_bytes_to_build_vector_index = 0, vector_index_parameter_check=0;

INSERT INTO test_multi_replica_status SELECT number as id, arrayMap(x -> (rand() % 10000000) / 10000000.0 * (if(rand() % 2 = 0, 1, -1)), range(768)) as vector, arrayMap(x -> (rand() % 10000000) / 10000000.0 * (if(rand() % 2 = 0, 1, -1)), range(768)) as v2 FROM numbers(1000);

ALTER TABLE test_multi_replica_status ADD VECTOR INDEX idx data TYPE MSTG('unknown=1');
ALTER TABLE test_multi_replica_status ADD VECTOR INDEX idx_v2 v2 TYPE MSTG('unknown=1');

SELECT if(status<>'InProgress', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_multi_replica_status' and name = 'idx_v2' and database = currentDatabase());
SELECT if(status<>'InProgress', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_multi_replica_status' and name = 'idx_v2' and database = currentDatabase());
SELECT if(status<>'InProgress', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_multi_replica_status' and name = 'idx_v2' and database = currentDatabase());

select table, name, expr, status, latest_failed_part, latest_fail_reason from system.vector_indices where database = currentDatabase() order by table;

SELECT 'After drop the first vector index idx';
ALTER TABLE test_multi_replica_status DROP VECTOR INDEX idx;

select table, name, expr, status, latest_failed_part, latest_fail_reason from system.vector_indices where database = currentDatabase() order by table;

SELECT 'After newly add again the first vector index idx';
ALTER TABLE test_multi_replica_status ADD VECTOR INDEX idx data TYPE MSTG;

select table, name, expr, status, latest_failed_part, latest_fail_reason from system.vector_indices where database = currentDatabase() order by table;

DROP TABLE test_multi_replica_status SYNC;

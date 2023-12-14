-- Tags: no-parallel

SELECT '-- Test drop a vector index and add a new index with the same name';
DROP TABLE IF EXISTS test_multi_replica_drop_index;
CREATE TABLE test_multi_replica_drop_index (`id` UInt32, `v1` Array(Float32), `v2` Array(Float32),
CONSTRAINT v1_len CHECK length(v1)=768, CONSTRAINT v2_len CHECK length(v2)=768)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_multi_replica_drop_index', '1') ORDER BY id;

INSERT INTO test_multi_replica_drop_index SELECT number, range(768), range(768) FROM numbers(500000);

ALTER TABLE test_multi_replica_drop_index ADD VECTOR INDEX v1 v1 TYPE MSTG;
ALTER TABLE test_multi_replica_drop_index ADD VECTOR INDEX v2 v2 TYPE MSTG;

-- wait build vector index to start
SELECT if(status='InProgress', sleep(2), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_multi_replica_drop_index' and name = 'v1' and database = currentDatabase());

ALTER TABLE test_multi_replica_drop_index DROP VECTOR INDEX v1, ADD VECTOR INDEX v1 v1 TYPE FLAT;

SELECT '-- status is InProgress for new added v1 after drop index v1';
SELECT table, name, type, expr, status FROM system.vector_indices where database = currentDatabase() and table = 'test_multi_replica_drop_index';

DROP TABLE test_multi_replica_drop_index SYNC;

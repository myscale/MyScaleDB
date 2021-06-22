DROP TABLE IF EXISTS test_drop_replica_table SYNC;

CREATE TABLE test_drop_replica_table(id UInt32, text String, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 768) Engine= ReplicatedMergeTree('/clickhouse/tables/default/test_drop_replica_table', '1') ORDER BY id;
INSERT INTO test_drop_replica_table SELECT number, randomPrintableASCII(80), range(768) FROM numbers(500000);

optimize table test_drop_replica_table final;

ALTER TABLE test_drop_replica_table ADD VECTOR INDEX v1 vector TYPE HNSWFLAT;
SELECT '-- Test drop index not blocked by concurently buiding vector index';
SELECT table, name, type, expr, status from system.vector_indices where database = currentDatabase() and table = 'test_drop_replica_table';

SELECT sleep(2);

ALTER TABLE test_drop_replica_table DROP VECTOR INDEX v1;
SELECT '-- Empty result, no vector index v1 after drop index';
SELECT table, name, type, expr, status FROM system.vector_indices where database = currentDatabase() and table = 'test_drop_replica_table';

ALTER TABLE test_drop_replica_table ADD VECTOR INDEX v2 vector TYPE HNSWPQ;
SELECT sleep(3);
SELECT '-- Test drop table can interrupt building vector index, not blocked by it';
SELECT table, name, type, expr, status FROM system.vector_indices where database = currentDatabase() and table = 'test_drop_replica_table';

DROP TABLE IF EXISTS test_drop_replica_table SYNC;

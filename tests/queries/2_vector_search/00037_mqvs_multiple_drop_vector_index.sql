-- Tags: no-parallel

SELECT '-- Test drop a vector index and add a new index with the same name';
DROP TABLE IF EXISTS test_multi_drop_index;
CREATE TABLE test_multi_drop_index (`id` UInt32, `v1` Array(Float32), `v2` Array(Float32),
CONSTRAINT v1_len CHECK length(v1)=768, CONSTRAINT v2_len CHECK length(v2)=768) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_multi_drop_index SELECT number, range(768), range(768) FROM numbers(500000);

ALTER TABLE test_multi_drop_index ADD VECTOR INDEX v1 v1 TYPE MSTG;
-- wait build vector index to start
SYSTEM WAIT BUILDING VECTOR INDICES test_multi_drop_index;

SYSTEM STOP MERGES test_multi_drop_index;
ALTER TABLE test_multi_drop_index DROP VECTOR INDEX v1, ADD VECTOR INDEX v1 v1 TYPE FLAT;

SELECT '-- status is InProgress for new added v1 after drop index v1';
SELECT table, name, type, expr, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_multi_drop_index';

DROP TABLE test_multi_drop_index SYNC;

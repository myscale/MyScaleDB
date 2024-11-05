-- Tags: no-parallel

DROP TABLE IF EXISTS test_replicated_vector;
CREATE TABLE test_replicated_vector(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/mqvs_00022/vector_insert', 'r1') primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1000;
ALTER TABLE test_replicated_vector ADD VECTOR INDEX v1 vector TYPE HNSWFLAT;
INSERT INTO test_replicated_vector SELECT number, [number, number, number] FROM numbers(2100);

set allow_experimental_lightweight_delete=1;
set mutations_sync=1;

delete from test_replicated_vector where id = 2;

SYSTEM WAIT BUILDING VECTOR INDICES test_replicated_vector;

SELECT id, vector, distance(vector, [0.1, 0.1, 0.1]) as d FROM test_replicated_vector order by d limit 10;

SELECT 'Test build vector index for new inserted part after lightweight delete';
INSERT INTO test_replicated_vector SELECT number, [number, number, number] FROM numbers(2100,1001);

SYSTEM WAIT BUILDING VECTOR INDICES test_replicated_vector;
SELECT status FROM system.vector_indices where database=currentDatabase() and table='test_replicated_vector';

DROP TABLE test_replicated_vector SYNC;

-- Tags: no-parallel

DROP TABLE IF EXISTS test_replicated_vector_merge SYNC;
CREATE TABLE test_replicated_vector_merge(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/mqvs_00022/vector_merge', 'r1') primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1, enable_rebuild_for_decouple=false;
ALTER TABLE test_replicated_vector_merge ADD VECTOR INDEX v1 vector TYPE HNSWFLAT;
INSERT INTO test_replicated_vector_merge SELECT number, [number, number, number] FROM numbers(100);

SYSTEM WAIT BUILDING VECTOR INDICES test_replicated_vector_merge;

SYSTEM STOP BUILD VECTOR INDICES test_replicated_vector_merge;
INSERT INTO test_replicated_vector_merge SELECT number, [number, number, number] FROM numbers(200,1000);

OPTIMIZE TABLE test_replicated_vector_merge FINAL;

SELECT 'Test merge on part with vector index and part w/o index built';
SELECT table, name from system.parts where database=currentDatabase() and table='test_replicated_vector_merge' and active;

SYSTEM START BUILD VECTOR INDICES test_replicated_vector_merge;
SYSTEM WAIT BUILDING VECTOR INDICES test_replicated_vector_merge;
SELECT 'Test merge on all parts with vector index built';
SELECT status from system.vector_indices where database=currentDatabase() and table='test_replicated_vector_merge';

OPTIMIZE TABLE test_replicated_vector_merge FINAL;

SELECT table, name from system.parts where database=currentDatabase() and table='test_replicated_vector_merge' and active;

INSERT INTO test_replicated_vector_merge SELECT number, [number, number, number] FROM numbers(1100,1000);

OPTIMIZE TABLE test_replicated_vector_merge FINAL;

SELECT 'Test merge on decoupled part';
SELECT table, name from system.parts where database=currentDatabase() and table='test_replicated_vector_merge' and active;

DROP TABLE test_replicated_vector_merge sync;

-- Tags: no-parallel, no-tsan, no-asan, no-msan, no-ubsan
-- no sanitizer tests because they are logical error, will abort the server

SELECT '-- Test table with multiple vector indices';

CREATE TABLE test_multi_same_vindex (`id` UInt32, `v1` Array(Float32), `v2` Array(Float32),
VECTOR INDEX v1 v1 TYPE MSTG, VECTOR INDEX v1 v2 TYPE MSTG,
CONSTRAINT v1_len CHECK length(v1)=3, CONSTRAINT v2_len CHECK length(v2)=3) ENGINE = MergeTree ORDER BY id; -- { serverError LOGICAL_ERROR }

CREATE TABLE test_multi_same_column (`id` UInt32, `v1` Array(Float32), `v2` Array(Float32),
VECTOR INDEX v1 v1 TYPE MSTG, VECTOR INDEX v2 v1 TYPE MSTG,
CONSTRAINT v1_len CHECK length(v1)=3, CONSTRAINT v2_len CHECK length(v2)=3) ENGINE = MergeTree ORDER BY id; -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS test_multi;
CREATE TABLE test_multi (`id` UInt32, `v1` Array(Float32), `v2` Array(Float32),
VECTOR INDEX v1 v1 TYPE MSTG, VECTOR INDEX v2 v2 TYPE MSTG,
CONSTRAINT v1_len CHECK length(v1)=3, CONSTRAINT v2_len CHECK length(v2)=3) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_to_build_vector_index=10000;

INSERT INTO test_multi SELECT number, [number, number, number], [number+100, number+100, number+100] FROM numbers(5500);

SYSTEM WAIT BUILDING VECTOR INDICES test_multi;

SELECT '-- Check build status for multiple vector indices';
SELECT name, type, expr, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_multi';

SELECT '-- Drop a vector index v1';
ALTER TABLE test_multi DROP VECTOR INDEX v1;
SELECT name, type, expr, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_multi';

SELECT '-- Check system table vector_index_segments';
SELECT part, name, status FROM system.vector_index_segments WHERE database = currentDatabase() and table = 'test_multi' order by name;

ALTER TABLE test_multi ADD VECTOR INDEX v1 v1 TYPE MSTG;
SYSTEM WAIT BUILDING VECTOR INDICES test_multi;

SELECT '-- After add a second index on VPart, check build status for multiple vector indices';
SELECT name, type, expr, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_multi';

SELECT '-- Insert a new part for the test of VParts -> DPart';
INSERT INTO test_multi SELECT number, [number, number, number], [number+100, number+100, number+100] FROM numbers(5500,5500);
SYSTEM WAIT BUILDING VECTOR INDICES test_multi;

SELECT 'Before decouple, two VParts with multiple vector indices';
SELECT name, type, expr, total_parts, parts_with_vector_index, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_multi';

SYSTEM STOP BUILD VECTOR INDICES test_multi;
OPTIMIZE TABLE test_multi FINAL;

SELECT '-- After decouple, check system table vector_index_segments';
SELECT name, part, owner_part, owner_part_id, status FROM system.vector_index_segments WHERE database = currentDatabase() and table = 'test_multi' order by name, part, owner_part_id;

SELECT '-- After decouple, check system table vector_indices';
SELECT name, type, expr, total_parts, parts_with_vector_index, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_multi';

SYSTEM START BUILD VECTOR INDICES test_multi;
SYSTEM WAIT BUILDING VECTOR INDICES test_multi;

SELECT '-- DPart->VPart, check system table vector_indices';
SELECT name, type, expr, total_parts, parts_with_vector_index, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_multi';

DROP TABLE IF EXISTS test_multi;

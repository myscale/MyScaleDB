-- Tags: no-parallel, no-tsan, no-asan, no-msan, no-ubsan
-- no sanitizer tests because they are logical error, will abort the server

SET enable_brute_force_vector_search=1;

SELECT '-- Test newly added vector column with multiple vector indices';
DROP TABLE IF EXISTS test_multi_249_new_column;
CREATE TABLE test_multi_249_new_column (`id` UInt32, `v1` Array(Float32),
CONSTRAINT v1_len CHECK length(v1)=3) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_multi_249_new_column SELECT number, [number, number, number] FROM numbers(100);

ALTER TABLE test_multi_249_new_column ADD VECTOR INDEX v1 v1 TYPE FLAT;

ALTER TABLE test_multi_249_new_column ADD COLUMN v2 Array(Float32) DEFAULT v1, ADD CONSTRAINT v2_len CHECK length(v2)=3;
ALTER TABLE test_multi_249_new_column ADD VECTOR INDEX v2 v2 TYPE FLAT;

SYSTEM WAIT BUILDING VECTOR INDICES test_multi_249_new_column; -- { serverError INVALID_VECTOR_INDEX }

SELECT '-- Build status for old part without newly added column';
SELECT table, name, expr, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_multi_249_new_column' and name = 'v2';

SELECT '-- Vector Search on old part without newly added column currently throws exception';
SELECT id, distance(v2, [1.0,1.0,1.0]) as d FROM test_multi_249_new_column ORDER BY (d,id) limit 5 SETTINGS enable_brute_force_vector_search=1; ; -- { serverError LOGICAL_ERROR }

SELECT '-- Materialize new column on on old part';
ALTER TABLE test_multi_249_new_column MATERIALIZE COLUMN v2 SETTINGS mutations_sync=2;

SYSTEM WAIT BUILDING VECTOR INDICES test_multi_249_new_column;

SELECT '-- Build status for new part with newly added column';
SELECT table, name, expr, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_multi_249_new_column' and name = 'v2';

SELECT '-- Vector Search on new part with newly added column';
SELECT id, distance(v2, [1.0,1.0,1.0]) as d FROM test_multi_249_new_column ORDER BY (d,id) limit 5;

DROP TABLE test_multi_249_new_column SYNC;

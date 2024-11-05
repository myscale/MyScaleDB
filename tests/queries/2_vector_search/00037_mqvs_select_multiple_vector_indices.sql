-- Tags: no-parallel

SET enable_brute_force_vector_search=1;

DROP TABLE IF EXISTS test_select_multi;
CREATE TABLE test_select_multi (id UInt32, v1 Array(Float32), v2 Array(Float32),
CONSTRAINT v1_len CHECK length(v1)=3, CONSTRAINT v2_len CHECK length(v2)=3) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_to_build_vector_index=10000;

INSERT INTO test_select_multi SELECT number, [number, number, number], [number+100, number+100, number+100] FROM numbers(5500);

SELECT 'Brute force on v1';
SELECT id, distance(v1, [1.0,1.0,1.0]) AS dist FROM test_select_multi ORDER BY dist,id LIMIT 10;

SELECT 'Brute force on v2';
SELECT id, distance(v2, [111.0,111.0,111.0]) AS dist FROM test_select_multi ORDER BY dist,id LIMIT 10;

ALTER TABLE test_select_multi ADD VECTOR INDEX v1 v1 TYPE MSTG;
ALTER TABLE test_select_multi ADD VECTOR INDEX v2 v2 TYPE FLAT;

SYSTEM WAIT BUILDING VECTOR INDICES test_select_multi;

SELECT 'Vector Index Status';
SELECT name, type, status FROM system.vector_indices WHERE database=currentDatabase() AND table='test_select_multi' order by name;

SELECT 'Vector Index Scan Search on v1 with MSTG';
SELECT id, distance(v1, [1.0,1.0,1.0]) AS dist FROM test_select_multi ORDER BY dist,id LIMIT 10;

SELECT 'Vector Index Scan Search on v2 with FLAT';
SELECT id, distance(v2, [111.0,111.0,111.0]) AS dist FROM test_select_multi ORDER BY dist,id LIMIT 10;

DROP TABLE IF EXISTS test_select_multi;

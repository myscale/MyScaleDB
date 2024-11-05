-- Tags: no-parallel

set enable_brute_force_vector_search = 1;

DROP TABLE IF EXISTS test_binary;
CREATE TABLE test_binary(id UInt32, vector FixedString(4)) engine MergeTree primary key id SETTINGS min_rows_to_build_vector_index=0, min_bytes_to_build_vector_index=0;
INSERT INTO test_binary SELECT number, char(number, number, number, number) FROM numbers(1024);

SELECT '-- Brute Force (Hamming)';
SELECT id, distance(vector, char(100, 101, 102, 103)) AS dist FROM test_binary ORDER BY dist,id LIMIT 20;

SELECT '-- Batch distance (Hamming)';
SELECT id, batch_distance(vector, [unbin('01010101010101010101010101010101'), char(0, 255, 1, 254), unhex('FFFFFFFF')]) AS dist FROM test_binary ORDER BY dist.1 ASC, dist.2 ASC, id ASC LIMIT 10 BY dist.1;

SELECT '-- Search with filter (Hamming)';
SELECT id, distance(vector, char(100, 101, 102, 103)) AS dist FROM test_binary WHERE id > 100 and id < 120 ORDER BY dist,id LIMIT 20;

SELECT '-- Brute Force (Jaccard)';
ALTER TABLE test_binary MODIFY SETTING binary_vector_search_metric_type = 'Jaccard';
SELECT id, distance(vector, char(100, 101, 102, 103)) AS dist FROM test_binary ORDER BY dist,id LIMIT 20;

SELECT '-- Batch distance (Jaccard)';
SELECT id, batch_distance(vector, [unbin('01010101010101010101010101010101'), char(0, 255, 1, 254), unhex('FFFFFFFF')]) AS dist FROM test_binary ORDER BY dist.1 ASC, dist.2 ASC, id ASC LIMIT 10 BY dist.1;

SELECT '-- Search with filter (Jaccard)';
SELECT id, distance(vector, char(100, 101, 102, 103)) AS dist FROM test_binary WHERE id > 100 and id < 120 ORDER BY dist,id LIMIT 20;

SELECT '-- BinaryFLAT (Hamming)';
ALTER TABLE test_binary ADD VECTOR INDEX vec_ind vector TYPE BinaryFLAT('metric_type=Hamming');
SYSTEM WAIT BUILDING VECTOR INDICES test_binary;
SELECT table, name, type, expr, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_binary';
SELECT id, distance(vector, char(100, 101, 102, 103)) AS dist FROM test_binary ORDER BY dist,id LIMIT 10;
ALTER TABLE test_binary DROP VECTOR INDEX vec_ind;

SELECT '-- BinaryFLAT (Jaccard)';
ALTER TABLE test_binary ADD VECTOR INDEX vec_ind vector TYPE BinaryFLAT('metric_type=Jaccard');
SYSTEM WAIT BUILDING VECTOR INDICES test_binary;
SELECT table, name, type, expr, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_binary';
SELECT id, distance(vector, char(100, 101, 102, 103)) AS dist FROM test_binary ORDER BY dist,id LIMIT 10;
ALTER TABLE test_binary DROP VECTOR INDEX vec_ind;

SELECT '-- BINARYMSTG (Hamming)';
ALTER TABLE test_binary ADD VECTOR INDEX vec_ind vector TYPE BINARYMSTG('metric_type=Hamming');
SYSTEM WAIT BUILDING VECTOR INDICES test_binary;
SELECT table, name, type, expr, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_binary';
SELECT id, distance(vector, char(100, 101, 102, 103)) AS dist FROM test_binary ORDER BY dist,id LIMIT 4;
ALTER TABLE test_binary DROP VECTOR INDEX vec_ind;

SELECT '-- BINARYMSTG (Jaccard)';
ALTER TABLE test_binary ADD VECTOR INDEX vec_ind vector TYPE BINARYMSTG('metric_type=Jaccard');
SYSTEM WAIT BUILDING VECTOR INDICES test_binary;
SELECT table, name, type, expr, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_binary';
SELECT id, distance(vector, char(100, 101, 102, 103)) AS dist FROM test_binary ORDER BY dist,id LIMIT 4;
ALTER TABLE test_binary DROP VECTOR INDEX vec_ind;

SELECT '-- LWD';
DELETE FROM test_binary WHERE id < 200;
ALTER TABLE test_binary MODIFY SETTING binary_vector_search_metric_type = 'Hamming';
SELECT id, distance(vector, char(100, 101, 102, 103)) AS dist FROM test_binary ORDER BY dist,id LIMIT 10;
ALTER TABLE test_binary ADD VECTOR INDEX vec_ind vector TYPE BINARYMSTG('metric_type=Jaccard');
SYSTEM WAIT BUILDING VECTOR INDICES test_binary;
SELECT table, name, type, expr, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_binary';
SELECT id, distance(vector, char(100, 101, 102, 103)) AS dist FROM test_binary ORDER BY dist,id LIMIT 4;

DROP TABLE IF EXISTS test_binary;
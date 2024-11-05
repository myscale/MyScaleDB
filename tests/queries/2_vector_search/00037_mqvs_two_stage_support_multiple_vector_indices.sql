-- Tags: no-parallel

SELECT 'Test two stage search with MSTG and IVFFLAT types';

DROP TABLE IF EXISTS test_vector_two_stage_multi SYNC;
CREATE TABLE test_vector_two_stage_multi
(
    id    UInt32,
    vector  Array(Float32),
    v2  Array(Float32),
    CONSTRAINT check_length CHECK length(vector) = 16,
    CONSTRAINT check_length_v2 CHECK length(v2) = 16
)
engine = MergeTree
ORDER BY id
SETTINGS min_bytes_to_build_vector_index=10000;

ALTER TABLE test_vector_two_stage_multi ADD VECTOR INDEX vec_ind vector TYPE MSTG;
ALTER TABLE test_vector_two_stage_multi ADD VECTOR INDEX vec_ind_v2 v2 TYPE IVFFLAT;

INSERT INTO test_vector_two_stage_multi SELECT number, [number,number,number,number,number,number,number,number,number,number,number,number,number,number,number,number], range(1,17) FROM numbers(1001) where number != 1;

SYSTEM WAIT BUILDING VECTOR INDICES test_vector_two_stage_multi;

SELECT 'Vector index build status';
SELECT name, type, expr, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_vector_two_stage_multi';

SELECT 'two stage search enabled on MSTG type';
SELECT id, distance(vector,[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]) AS dist FROM test_vector_two_stage_multi WHERE id < 11 ORDER BY dist, id LIMIT 10 settings two_stage_search_option=2;

SELECT 'two stage search enabled on IVFFLAT type';
SELECT id, distance(v2,[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0,11.0,12.0,13.0,14.0,15.0,16.0]) AS dist FROM test_vector_two_stage_multi WHERE id < 11 ORDER BY dist, id LIMIT 10 settings two_stage_search_option=2;

DROP TABLE IF EXISTS test_vector_two_stage_multi SYNC;

-- Tags: no-parallel

DROP TABLE IF EXISTS test_vector_two_stage SYNC;
CREATE TABLE test_vector_two_stage
(
    id    UInt32,
    vec  Array(Float32),
    vec_without_idx  Array(Float32),
    CONSTRAINT check_length_vec CHECK length(vec) = 16,
    CONSTRAINT check_length_vec_without_idx CHECK length(vec_without_idx) = 16
)
engine = MergeTree
ORDER BY id
SETTINGS default_mstg_disk_mode=2, min_bytes_to_build_vector_index=0;

INSERT INTO test_vector_two_stage 
SELECT 
    number,
    [number,number,number,number,number,number,number,number,number,number,number,number,number,number,number,number],
    [number,number,number,number,number,number,number,number,number,number,number,number,number,number,number,number]
FROM
    numbers(500001)
where number != 1;


select 'IP Distances';

ALTER TABLE test_vector_two_stage modify setting float_vector_search_metric_type = 'IP';
ALTER TABLE test_vector_two_stage ADD VECTOR INDEX ip_vidx vec TYPE MSTG;

SYSTEM WAIT BUILDING VECTOR INDICES test_vector_two_stage;

SELECT 'Vector index build status';
SELECT name, type, expr, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_vector_two_stage';

SELECT 'Without Two Stage Search';

SET enable_brute_force_vector_search = 0;
Set two_stage_search_option=0;

SELECT 
    id,
    distance(
        vec,
        [1.0,1.5,2.0,1.9,1.1,10.0,12.0,11.5,1.4,14.1,30.0,44.0,55.0,66.0,77.0,88.0]
    ) AS dist 
FROM test_vector_two_stage
WHERE id > 50 and id < 80
ORDER BY dist DESC, id
LIMIT 10;

SELECT 'With Two Stage Search';

Set two_stage_search_option=2;

SELECT 
    id,
    distance(
        vec,
        [1.0,1.5,2.0,1.9,1.1,10.0,12.0,11.5,1.4,14.1,30.0,44.0,55.0,66.0,77.0,88.0]
    ) AS dist 
FROM test_vector_two_stage
WHERE id > 50 and id < 80
ORDER BY dist DESC, id
LIMIT 10;

SELECT 'Brute Force Search';
SET enable_brute_force_vector_search = 1;

SELECT 
    id,
    distance(
        vec_without_idx,
        [1.0,1.5,2.0,1.9,1.1,10.0,12.0,11.5,1.4,14.1,30.0,44.0,55.0,66.0,77.0,88.0]
    ) AS dist 
FROM test_vector_two_stage
WHERE id > 50 and id < 80
ORDER BY dist DESC, id
LIMIT 10;

ALTER TABLE test_vector_two_stage DROP VECTOR INDEX ip_vidx;

select 'L2 Distances';

ALTER TABLE test_vector_two_stage modify setting float_vector_search_metric_type = 'L2';
ALTER TABLE test_vector_two_stage ADD VECTOR INDEX l2_vidx vec TYPE MSTG;

SYSTEM WAIT BUILDING VECTOR INDICES test_vector_two_stage;

SELECT 'Vector index build status';
SELECT name, type, expr, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_vector_two_stage';

SELECT 'Without Two Stage Search';

Set two_stage_search_option=0;
SET enable_brute_force_vector_search = 0;

SELECT 
    id,
    distance(
        vec,
        [1.0,1.5,2.0,1.9,1.1,10.0,12.0,11.5,1.4,14.1,30.0,44.0,55.0,66.0,77.0,88.0]
    ) AS dist 
FROM test_vector_two_stage
WHERE id > 50 and id < 80
ORDER BY dist, id 
LIMIT 10;

SELECT 'With Two Stage Search';
Set two_stage_search_option=2;

SELECT 
    id,
    distance(
        vec,
        [1.0,1.5,2.0,1.9,1.1,10.0,12.0,11.5,1.4,14.1,30.0,44.0,55.0,66.0,77.0,88.0]
    ) AS dist 
FROM test_vector_two_stage
WHERE id > 50 and id < 80
ORDER BY dist, id 
LIMIT 10;

SELECT 'Brute Force Search';
SET enable_brute_force_vector_search = 1;

SELECT 
    id,
    distance(
        vec_without_idx,
        [1.0,1.5,2.0,1.9,1.1,10.0,12.0,11.5,1.4,14.1,30.0,44.0,55.0,66.0,77.0,88.0]
    ) AS dist 
FROM test_vector_two_stage
WHERE id > 50 and id < 80
ORDER BY dist, id 
LIMIT 10;

ALTER TABLE test_vector_two_stage DROP VECTOR INDEX l2_vidx;

select 'Cosine Distances';

ALTER TABLE test_vector_two_stage modify setting float_vector_search_metric_type = 'cosine';
ALTER TABLE test_vector_two_stage ADD VECTOR INDEX cosine_vidx vec TYPE MSTG;

SYSTEM WAIT BUILDING VECTOR INDICES test_vector_two_stage;

SELECT 'Vector index build status';
SELECT name, type, expr, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_vector_two_stage';

SELECT 'Without Two Stage Search';
Set two_stage_search_option=0;
SET enable_brute_force_vector_search = 0;

SELECT 
    id,
    distance(
        vec,
        [1.0,1.5,2.0,1.9,1.1,10.0,12.0,11.5,1.4,14.1,30.0,44.0,55.0,66.0,77.0,88.0]
    ) AS dist
FROM test_vector_two_stage
WHERE id > 50 and id < 80
ORDER BY dist, id
LIMIT 10;

SELECT 'With Two Stage Search';
Set two_stage_search_option=2;

SELECT 
    id,
    distance(
        vec,
        [1.0,1.5,2.0,1.9,1.1,10.0,12.0,11.5,1.4,14.1,30.0,44.0,55.0,66.0,77.0,88.0]
    ) AS dist
FROM test_vector_two_stage
WHERE id > 50 and id < 80
ORDER BY dist, id
LIMIT 10;

SELECT 'Brute Force Search';
SET enable_brute_force_vector_search = 1;

SELECT 
    id,
    distance(
        vec_without_idx,
        [1.0,1.5,2.0,1.9,1.1,10.0,12.0,11.5,1.4,14.1,30.0,44.0,55.0,66.0,77.0,88.0]
    ) AS dist
FROM test_vector_two_stage
WHERE id > 50 and id < 80
ORDER BY dist, id
LIMIT 10;

ALTER TABLE test_vector_two_stage DROP VECTOR INDEX cosine_vidx;

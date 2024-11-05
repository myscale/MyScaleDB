-- Tags: no-parallel

DROP TABLE IF EXISTS test_multi_distances;
CREATE TABLE test_multi_distances(
    id UInt32, v1 Array(Float32), v2 Array(Float32),
    CONSTRAINT check_length_v1 CHECK length(v1) = 3,
    CONSTRAINT check_length_v2 CHECK length(v2) = 3
) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_multi_distances SELECT number, [number, number, number], [number+1, number+1, number+1] FROM numbers(100);

ALTER TABLE test_multi_distances ADD vector index v1_idx v1 TYPE IVFFLAT;
ALTER TABLE test_multi_distances ADD vector index v2_idx v2 TYPE IVFFLAT;
SYSTEM WAIT BUILDING VECTOR INDICES test_multi_distances;

-- Not support multiple distances mixed with text or hybrid search
SELECT id, distance(v1,[0.0,0,0]) as dist, textsearch(text,'keyword') as bm25 FROM test_multi_distances ORDER BY dist LIMIT 1; -- { serverError NOT_IMPLEMENTED }
SELECT id, hybridsearch(v1,text,[0.0,0,0],'keyword') as hybrid_score, textsearch(text,'keyword') as bm25 FROM test_multi_distances ORDER BY hybrid_score LIMIT 1; -- { serverError NOT_IMPLEMENTED }
SELECT id, distance(v1,[0.0,0,0]) as dist FROM test_multi_distances ORDER BY textsearch(text,'keyword') LIMIT 1; -- { serverError NOT_IMPLEMENTED }

SELECT 'mutiple distances order by dist1 + dist2';
SELECT id, distance(v1,[0.,0,0]) as dist1, distance(v2,[3.,3,3]) as dist2
FROM test_multi_distances
ORDER BY dist1 + dist2
LIMIT 5;

SELECT 'mutiple distances with distances_top_k_multiply_factor=1';
SELECT id, distance(v1,[0.,0,0]) as dist1, distance(v2,[3.,3,3]) as dist2
FROM test_multi_distances
ORDER BY dist1 + dist2
LIMIT 5 SETTINGS distances_top_k_multiply_factor=1;

SELECT 'mutiple distances order by dist1';
SELECT id, distance(v1,[0.,0,0]) as dist1, distance(v2,[3.,3,3]) as dist2
FROM test_multi_distances
ORDER BY dist1
LIMIT 5;

SELECT 'multiple distances no order by (default _part, _part_offset)';
SELECT id, distance(v1,[0.,0,0]) as dist1, distance(v2,[3.,3,3]) as dist2
FROM test_multi_distances
LIMIT 5;

SELECT 'mutiple distances order by dist1 + dist2 (different labels, default NULL)';
SELECT id, distance(v2,[50.,50,50]) as dist1, distance(v1,[25.,25,25]) as dist2
FROM test_multi_distances
ORDER BY dist1 + dist2
LIMIT 5;

SELECT 'multiple distances with WHERE clause';
SELECT id, distance(v1,[0.,0,0]) as dist1, distance(v2,[3.,3,3]) as dist2
FROM test_multi_distances
WHERE id > 2
ORDER BY dist1 + dist2
LIMIT 5;

DELETE FROM test_multi_distances WHERE id=4;

SELECT 'multiple distances with WHERE clause after LWD';
SELECT id, distance(v1,[0.,0,0]) as dist1, distance(v2,[3.,3,3]) as dist2
FROM test_multi_distances
WHERE id > 2
ORDER BY dist1 + dist2
LIMIT 5;

DROP TABLE IF EXISTS t_00050;
CREATE TABLE t_00050(a int, id UInt64) engine=MergeTree ORDER BY a;
INSERT INTO t_00050 SELECT number, number FROM numbers(10);

SELECT 'multiple distances on right joined table';
SELECT t1.id, t1.a, distance(t2.v1,[0.,0,0]) as dist1, distance(v2,[3.,3,3]) as dist2
FROM t_00050 as t1 JOIN test_multi_distances as t2 ON t1.id = t2.id
ORDER BY dist1 + dist2, t1.a
LIMIT 5;

SELECT 'multiple distances in subquery';
SELECT id FROM (
    SELECT id, distance(v1,[0.,0,0]) as dist1, distance(v2,[3.,3,3]) as dist2
    FROM test_multi_distances
    ORDER BY dist1 + dist2
    LIMIT 1
);

SELECT 'multiple distances with subquery and WITH clause as query vector';
WITH
(
    SELECT v1 FROM test_multi_distances LIMIT 1
) AS query_v1
SELECT id, distance(v1, query_v1) as dist1, distance(v2, (SELECT v2 FROM test_multi_distances LIMIT 1)) as dist2
FROM test_multi_distances
ORDER BY dist1 + dist2
LIMIT 5;

DROP TABLE IF EXISTS t_00050;
DROP TABLE IF EXISTS test_multi_distances;

-- two parts
DROP TABLE IF EXISTS test_multi_distances_parts;
CREATE TABLE test_multi_distances_parts(
    id UInt32, v1 Array(Float32), v2 Array(Float32),
    CONSTRAINT check_length_v1 CHECK length(v1) = 3,
    CONSTRAINT check_length_v2 CHECK length(v2) = 3
) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_multi_distances_parts SELECT number, [number, number, number], [number+1, number+1, number+1] FROM numbers(100) where number%2==0;
INSERT INTO test_multi_distances_parts SELECT number, [number, number, number], [number+1, number+1, number+1] FROM numbers(100) where number%2==1;

SELECT 'brute force on multiple distances';
SELECT id, distance(v1,[0.,0,0]) as dist1, distance(v2,[3.,3,3]) as dist2
FROM test_multi_distances_parts
ORDER BY dist1 + dist2
LIMIT 5 SETTINGS enable_brute_force_vector_search=1;

SELECT 'brute force on multiple distances on two parts and where condition filter out the first part';
SELECT id, distance(v1,[0.,0,0]) as dist1, distance(v2,[3.,3,3]) as dist2
FROM test_multi_distances_parts
WHERE _part = 'all_2_2_0'
ORDER BY dist1 + dist2
LIMIT 5 SETTINGS enable_brute_force_vector_search=1;

ALTER TABLE test_multi_distances_parts ADD vector index v1_idx v1 TYPE IVFFLAT;
ALTER TABLE test_multi_distances_parts ADD vector index v2_idx v2 TYPE IVFFLAT;
SYSTEM WAIT BUILDING VECTOR INDICES test_multi_distances_parts;

SELECT 'mutiple distances order by dist1 + dist2 on two parts';
SELECT id, distance(v1,[0.,0,0]) as dist1, distance(v2,[3.,3,3]) as dist2
FROM test_multi_distances_parts
ORDER BY dist1 + dist2
LIMIT 5;

SELECT 'mutiple distances no order by (default _part, _part_offset)';
SELECT id, distance(v1,[0.,0,0]) as dist1, distance(v2,[3.,3,3]) as dist2
FROM test_multi_distances_parts
LIMIT 5 SETTINGS distances_top_k_multiply_factor=1;

SELECT 'mutiple distances with WHERE clause on two parts';
SELECT id, distance(v1,[0.,0,0]) as dist1, distance(v2,[3.,3,3]) as dist2
FROM test_multi_distances_parts
WHERE id > 2
ORDER BY dist1 + dist2
LIMIT 5;

DELETE FROM test_multi_distances_parts WHERE id=4;

SELECT 'mutiple distances on two parts after LWD';
SELECT id, distance(v1,[0.,0,0]) as dist1, distance(v2,[3.,3,3]) as dist2
FROM test_multi_distances_parts
ORDER BY dist1 + dist2
LIMIT 5;

DROP TABLE IF EXISTS test_multi_distances_parts;

-- primary key cache
DROP TABLE IF EXISTS test_multi_distances_pkcache;
CREATE TABLE test_multi_distances_pkcache(
    id UInt32, v1 Array(Float32), v2 Array(Float32),
    CONSTRAINT check_length_v1 CHECK length(v1) = 3,
    CONSTRAINT check_length_v2 CHECK length(v2) = 3
) ENGINE = MergeTree ORDER BY id SETTINGS enable_primary_key_cache=true;

INSERT INTO test_multi_distances_pkcache SELECT number, [number, number, number], [number+1, number+1, number+1] FROM numbers(100);

ALTER TABLE test_multi_distances_pkcache ADD vector index v1_idx v1 TYPE IVFFLAT;
ALTER TABLE test_multi_distances_pkcache ADD vector index v2_idx v2 TYPE IVFFLAT;
SYSTEM WAIT BUILDING VECTOR INDICES test_multi_distances_pkcache;

SELECT 'mutiple distances with primary key cache';
SELECT id, distance(v1,[0.,0,0]) as dist1, distance(v2,[3.,3,3]) as dist2
FROM test_multi_distances_pkcache
ORDER BY dist1 + dist2
LIMIT 5;

SELECT 'mutiple distances + _part_offset with primary key cache';
SELECT id, _part_offset, distance(v1,[0.,0,0]) as dist1, distance(v2,[3.,3,3]) as dist2
FROM test_multi_distances_pkcache
ORDER BY dist1 + dist2
LIMIT 5;

DELETE FROM test_multi_distances_pkcache WHERE id=2;

SELECT 'mutiple distances with primary key cache after LWD';
SELECT id, distance(v1,[0.,0,0]) as dist1, distance(v2,[3.,3,3]) as dist2
FROM test_multi_distances_pkcache
ORDER BY dist1 + dist2
LIMIT 5;

DROP TABLE IF EXISTS test_multi_distances_pkcache;

-- binary vector
DROP TABLE IF EXISTS test_multi_distances_binary;
CREATE TABLE test_multi_distances_binary(
    id UInt32, v1 FixedString(4), v2 Array(Float32), v3 FixedString(3),
    CONSTRAINT check_length_v2 CHECK length(v2) = 3
) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_multi_distances_binary SELECT number, char(number, number, number, number), [number+1, number+1, number+1], char(number+2, number+2, number+2) FROM numbers(100);

ALTER TABLE test_multi_distances_binary ADD VECTOR INDEX vec_ind1 v1 TYPE BinaryFLAT('metric_type=Hamming');
ALTER TABLE test_multi_distances_binary ADD VECTOR INDEX vec_ind2 v2 TYPE IVFFLAT;
ALTER TABLE test_multi_distances_binary ADD VECTOR INDEX vec_ind3 v3 TYPE BinaryFLAT('metric_type=Jaccard');
SYSTEM WAIT BUILDING VECTOR INDICES test_multi_distances_binary;

SELECT 'multiple distances with binary vectors';
SELECT id, distance(v1,char(0.,0,0,0)) as dist1, distance(v3,char(4.,4,4)) as dist2, distance(v2, [3.0,3,3]) as dist3
FROM test_multi_distances_binary
ORDER BY dist1 + dist2 + dist3
LIMIT 5;

DROP TABLE IF EXISTS test_multi_distances_binary;

-- two stage search
DROP TABLE IF EXISTS test_multi_distances_two_stage;
CREATE TABLE test_multi_distances_two_stage(
    id UInt32, v1 Array(Float32), v2 Array(Float32), v3 Array(Float32),
    CONSTRAINT check_length_v1 CHECK length(v1) = 16,
    CONSTRAINT check_length_v2 CHECK length(v2) = 16,
    CONSTRAINT check_length_v2 CHECK length(v3) = 8
) ENGINE = MergeTree ORDER BY id SETTINGS default_mstg_disk_mode=1;

ALTER TABLE test_multi_distances_two_stage ADD VECTOR INDEX vec_ind1 v1 TYPE MSTG;
ALTER TABLE test_multi_distances_two_stage ADD VECTOR INDEX vec_ind2 v2 TYPE MSTG;
ALTER TABLE test_multi_distances_two_stage ADD VECTOR INDEX vec_ind3 v3 TYPE IVFFLAT;

INSERT INTO test_multi_distances_two_stage SELECT number, arrayWithConstant(16, number), arrayWithConstant(16, number+1), arrayWithConstant(8, number+2) FROM numbers(1001);

SYSTEM WAIT BUILDING VECTOR INDICES test_multi_distances_two_stage;

SELECT 'multiple distances with two stage search enabled on MSTG type';
SELECT id, distance(v1, arrayWithConstant(16, 0.0)) as dist1, distance(v2, arrayWithConstant(16, 3.0)) as dist2
FROM test_multi_distances_two_stage
ORDER BY dist1 + dist2
LIMIT 5 SETTINGS two_stage_search_option=2;

SELECT 'multiple distances with two stage search enable on MSTG and IVFFLAT type';
SELECT id, distance(v1, arrayWithConstant(16, 0.0)) as dist1, distance(v3, arrayWithConstant(8, 6.0)) as dist2
FROM test_multi_distances_two_stage
ORDER BY dist1 + dist2
LIMIT 5 SETTINGS two_stage_search_option=2;

DROP TABLE IF EXISTS test_multi_distances_two_stage;

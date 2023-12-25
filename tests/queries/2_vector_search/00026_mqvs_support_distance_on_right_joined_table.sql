
-- Tags: no-parallel

SET enable_brute_force_vector_search=1;

DROP TABLE IF EXISTS test_vector_join;
CREATE TABLE test_vector_join
(
    id    UInt32,
    vector  Array(Float32),
    CONSTRAINT check_length CHECK length(vector) = 3
)
engine = MergeTree ORDER BY id;

INSERT INTO test_vector_join SELECT number, [number, number, number] from numbers(1, 100);

DROP TABLE IF EXISTS t_00026;
CREATE TABLE t_00026(a int, id int) engine=MergeTree ORDER BY a;
INSERT INTO t_00026 SELECT number, number FROM numbers(10);
INSERT INTO t_00026 SELECT number+10, number FROM numbers(10);

SELECT t1.id, t2.a, distance(t1.vector, [1.0,1.0,1.0]) as dist
FROM test_vector_join as t1 JOIN t_00026 as t2 ON t1.id = t2.id
ORDER BY dist, t2.a
LIMIT 10;

SELECT t1.id, t1.a, distance(t2.vector, [1.0,1.0,1.0]) as dist
FROM t_00026 as t1 JOIN test_vector_join as t2 ON t1.id = t2.id
ORDER BY dist, t1.a
LIMIT 10;

SELECT t1.id, t1.a, distance(vector, [1.0,1.0,1.0]) as dist
FROM t_00026 as t1 JOIN test_vector_join ON t1.id = test_vector_join.id
ORDER BY dist, t1.a
LIMIT 10;

DROP TABLE t_00026;
DROP TABLE test_vector_join;

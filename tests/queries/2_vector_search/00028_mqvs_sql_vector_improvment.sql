
-- Tags: no-parallel

DROP TABLE IF EXISTS test_vector_sql_improvment;
CREATE TABLE test_vector_sql_improvment
(
    id    UInt32,
    vector  Array(Float32),
    CONSTRAINT check_length CHECK length(vector) = 3
)
engine = MergeTree ORDER BY id;

DROP TABLE IF EXISTS test_vector_bak;
CREATE TABLE test_vector_bak
(
    id    UInt32,
    vector  Array(Float32)
)
engine = MergeTree ORDER BY id;

ALTER TABLE test_vector_sql_improvment ADD VECTOR INDEX v2 vector TYPE HNSWFLAT;

INSERT INTO test_vector_sql_improvment SELECT number, [number, number, number] from numbers(1, 100);
INSERT INTO test_vector_bak SELECT number, [number, number, number] from numbers(1, 100);

SYSTEM WAIT BUILDING VECTOR INDICES test_vector_sql_improvment;
SYSTEM WAIT BUILDING VECTOR INDICES test_vector_bak;

SELECT 'vector column name exists in both two joined tables';
SELECT t1.id, distance(t2.vector, [1.0,1.0,1.0]) as dist
FROM test_vector_bak as t1
JOIN test_vector_sql_improvment as t2 ON t1.id = t2.id
ORDER BY dist LIMIT 5;

SELECT 'distance function column exists in GROUP BY clause';
SELECT id, distance(vector, [1.0,1.0,1.0]) as dist, count(*)
FROM test_vector_sql_improvment
GROUP BY id,dist
ORDER BY dist LIMIT 5;

SELECT 'distance function exists when main table is subquery';
SELECT id, distance(vector, [1.0,1.0,1.0]) as dist
FROM (
    SELECT id, vector FROM test_vector_sql_improvment WHERE id > 10)
GROUP BY id,dist
ORDER BY dist LIMIT 5;

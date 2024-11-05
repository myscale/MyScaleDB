
-- Tags: no-parallel

DROP TABLE IF EXISTS test_with_clause_process_function;
CREATE TABLE test_with_clause_process_function
(
    id    UInt32,
    vector  Array(Float32),
    CONSTRAINT check_length CHECK length(vector) = 3
)
engine = MergeTree PRIMARY KEY id;

INSERT INTO test_with_clause_process_function SELECT number, [number,number,number] FROM numbers(100);

ALTER TABLE test_with_clause_process_function ADD VECTOR INDEX vec_ind vector TYPE HNSWFLAT;

SYSTEM WAIT BUILDING VECTOR INDICES test_with_clause_process_function;

SELECT 'Lambda function in distance function';
SELECT id, distance(vector, arrayMap(x -> (x / 1.), range(1, 4))) AS d
FROM test_with_clause_process_function
ORDER BY (d, id)
LIMIT 5;

SELECT 'Lambda function inside WITH clause';
WITH(
        arrayMap(x -> ((x / 100.) * if((x % 2) = 0, -1, 1)), range(1, 4))
    ) AS generated_vector
SELECT
    id,
    distance(vector, generated_vector) AS d
FROM test_with_clause_process_function
ORDER BY (d, id) ASC
LIMIT 5;

SELECT 'Scalar Subquery inside WITH clause in distance function';
WITH(
        SELECT arrayMap(x -> (x / 1.), range(1, 4))
    ) AS target_vector
SELECT id FROM (
SELECT id, distance(vector, target_vector) AS dist
FROM test_with_clause_process_function
ORDER BY (dist, id)
LIMIT 10
);

DROP TABLE test_with_clause_process_function;

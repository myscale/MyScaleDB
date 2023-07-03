
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

SELECT sleep(2);

SELECT 'Lambda function in distance function';
select id, distance(vector, arrayMap(x -> (x / 1.), range(1, 4))) AS d
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
ORDER BY d ASC
LIMIT 5;

SELECT 'Scalar Subquery inside WITH clause in distance function';
WITH(
        SELECT arrayMap(x -> (x / 1.), range(1, 4))
    ) AS target_vector
select id FROM (
select id, distance(vector, target_vector) as dist
from test_with_clause_process_function
order by dist
limit 10
);

DROP TABLE test_with_clause_process_function;


-- Tags: no-parallel

DROP TABLE IF EXISTS test_vector_subquery;
CREATE TABLE test_vector_subquery
(
    id    UInt32,
    vector  Array(Float32),
    CONSTRAINT check_length CHECK length(vector) = 3
)
engine = MergeTree PRIMARY KEY id;

INSERT INTO test_vector_subquery SELECT number, [number,number,number] FROM numbers(1000);

ALTER TABLE test_vector_subquery ADD VECTOR INDEX vec_ind vector TYPE HNSWFLAT;

SYSTEM WAIT BUILDING VECTOR INDICES test_vector_subquery;

SELECT 'Scalar Subquery in distance function';
select id FROM (
select id, distance(vector, (
    select arrayMap(x->CAST(x AS Float64), vector)
    FROM test_vector_subquery
    LIMIT 1)
) as dist
from test_vector_subquery
order by dist
limit 10
)
limit 10;

SELECT 'Scalar Subquery with float32 data type in distance function';
select id FROM (
select id, distance(vector, (
    select vector
    FROM test_vector_subquery
    LIMIT 1)
) as dist
from test_vector_subquery
order by dist
limit 10
)
limit 10;

SELECT 'Scalar Subquery with float32 data type in batch distance function';
select id FROM (
select id, batch_distance(vector, (
    select [vector,vector]
    FROM test_vector_subquery
    LIMIT 1)
) as dist
from test_vector_subquery
order by dist.1, dist.2
limit 5 by dist.1
)
limit 10;

SELECT 'Scalar Subquery inside WITH clause in distance function';
WITH
(
    select arrayMap(x->CAST(x AS Float64), vector)
    FROM test_vector_subquery
    LIMIT 1
) AS target_vector
select id FROM (
select id, distance(vector, target_vector) as dist
from test_vector_subquery
order by dist
limit 10
);

SELECT 'Test remove unneeded distance function column in subquery';
SELECT id FROM (
    SELECT id, distance(vector, [1.0,1.0,1.0]) as dist
    FROM test_vector_subquery
    order by dist
    limit 1
);

DROP TABLE test_vector_subquery;

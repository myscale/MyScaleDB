
-- Tags: no-parallel

SET enable_brute_force_vector_search=1;

DROP TABLE IF EXISTS test_vector_metric_type;
CREATE TABLE test_vector_metric_type
(
    id    UInt32,
    vector  Array(Float32),
    CONSTRAINT check_length CHECK length(vector) = 3
)
engine = MergeTree ORDER BY id;

INSERT INTO test_vector_metric_type SELECT number, [number, number, number] from numbers(1, 100);

SELECT 'No vector index';
SELECT id, distance(vector, [1.0, 1.0, 1.0]) as d FROM test_vector_metric_type order by d limit 2;
SELECT id, distance(vector, [1.0, 1.0, 1.0]) as d FROM test_vector_metric_type order by d DESC limit 2; -- { serverError 62 }

SELECT 'metric_type=L2';
ALTER TABLE test_vector_metric_type ADD VECTOR INDEX v2 vector TYPE HNSWFLAT('metric_type=L2');

SELECT id, distance(vector, [1.0, 1.0, 1.0]) as d FROM test_vector_metric_type order by d limit 2;
SELECT id, distance(vector, [1.0, 1.0, 1.0]) as d FROM test_vector_metric_type order by d DESC limit 2; -- { serverError 62 }
ALTER TABLE test_vector_metric_type DROP VECTOR INDEX v2;

SELECT 'metric_type=IP';
ALTER TABLE test_vector_metric_type ADD VECTOR INDEX v2 vector TYPE HNSWFLAT('metric_type=IP');

SELECT id, distance(vector, [1.0, 1.0, 1.0]) as d FROM test_vector_metric_type order by d DESC limit 2;
SELECT id, distance(vector, [1.0, 1.0, 1.0]) as d FROM test_vector_metric_type order by d limit 2; -- { serverError 62 }

DROP TABLE test_vector_metric_type;

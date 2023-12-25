
-- Tags: no-parallel

DROP TABLE IF EXISTS test_vector_metric_type;
CREATE TABLE test_vector_metric_type
(
    id UInt32,
    vector Array(Float32),
    CONSTRAINT check_length CHECK length(vector) = 3
)
engine = MergeTree ORDER BY id;

INSERT INTO test_vector_metric_type SELECT number, [number, number, number] from numbers(1, 100);

DROP TABLE IF EXISTS test_distributed;
CREATE TABLE test_distributed (id UInt32, vector Array(Float32), CONSTRAINT check_length CHECK length(vector) = 3) ENGINE=Distributed(test_shard_localhost, currentDatabase(), 'test_vector_metric_type');

SELECT 'metric_type=L2';
ALTER TABLE test_vector_metric_type ADD VECTOR INDEX v2 vector TYPE HNSWFLAT('metric_type=L2');
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)) FROM (select status from system.vector_indices where table = 'test_vector_metric_type' and database = currentDatabase());

SELECT id, distance(vector, [1.0, 1.0, 1.0]) as d FROM test_distributed order by d limit 2;
SELECT id, distance(vector, [1.0, 1.0, 1.0]) as d FROM test_distributed order by d DESC limit 2; -- { serverError 62 }
ALTER TABLE test_vector_metric_type DROP VECTOR INDEX v2;

SELECT 'metric_type=IP';
ALTER TABLE test_vector_metric_type ADD VECTOR INDEX v2 vector TYPE HNSWFLAT('metric_type=IP');
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)) FROM (select status from system.vector_indices where table = 'test_vector_metric_type' and database = currentDatabase());

SELECT id, distance(vector, [1.0, 1.0, 1.0]) as d FROM test_distributed order by d DESC limit 2;
SELECT id, distance(vector, [1.0, 1.0, 1.0]) as d FROM test_distributed order by d limit 2; -- { serverError 62 }

DROP TABLE test_vector_metric_type;
DROP TABLE test_distributed;

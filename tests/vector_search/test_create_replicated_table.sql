DROP TABLE IF EXISTS test_vector_local;

CREATE TABLE test_vector_local(id UInt64, vector FixedArray(Float32, 3))
ENGINE=ReplicatedMergeTree('/clickhouse/databases/default/tables/test_vector', 'replica1')
ORDER BY id;
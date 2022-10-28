DROP TABLE IF EXISTS test_vector_local;

CREATE TABLE test_vector_local(id UInt64, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3)
ENGINE=ReplicatedMergeTree('/clickhouse/databases/default/tables/test_vector', 'replica1')
ORDER BY id;
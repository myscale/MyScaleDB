CREATE TABLE test_vector_local ON CLUSTER {cluster}(
    id UInt64, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_vector_local', '{replica}')
ORDER BY id;
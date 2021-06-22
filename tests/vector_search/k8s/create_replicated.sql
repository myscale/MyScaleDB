CREATE TABLE test_vector_local ON CLUSTER {cluster}(
    id UInt64, vector FixedArray(Float32, 3)
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_vector_local', '{replica}')
ORDER BY id;
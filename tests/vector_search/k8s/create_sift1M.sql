CREATE TABLE test_sift1M_local ON CLUSTER '{cluster}'
(
    id UInt64, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 128
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_sift1M_local', '{replica}')
ORDER BY id;

CREATE TABLE distributed_test_sift1M ON CLUSTER '{cluster}'
(
    id UInt64, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 128
) ENGINE = Distributed('{cluster}', default, test_sift1M_local, rand());
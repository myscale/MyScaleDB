CREATE TABLE test_sift10M_local ON CLUSTER testing
(
    id UInt64, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 128
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_sift10M_local', '{replica}')
ORDER BY id;

CREATE TABLE distributed_test_sift10M ON CLUSTER testing
(
    id UInt64, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 128
) ENGINE = Distributed(testing, default, test_sift10M_local, rand());
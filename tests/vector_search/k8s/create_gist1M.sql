CREATE TABLE test_gist1M_local ON CLUSTER '{cluster}'
(
    id UInt64, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 960
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_gist1M_local', '{replica}')
ORDER BY id;

CREATE TABLE distributed_test_gist1M ON CLUSTER '{cluster}'
(
    id UInt64, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 960
) ENGINE = Distributed('{cluster}', default, test_gist1M_local, rand());
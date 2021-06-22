CREATE TABLE test_vector_local ON CLUSTER test_cluster_two_shards(
    id UInt64, vector FixedArray(Float32, 3)
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_vector_local', '{replica}')
ORDER BY id;

CREATE TABLE distributed_test_vector ON CLUSTER
test_cluster_two_shards
(
    id UInt64, vector FixedArray(Float32, 3)
) ENGINE = Distributed(test_cluster_two_shards, default, test_vector_local, rand());

INSERT INTO distributed_test_vector SELECT number, [number, number, number] FROM numbers(100000);

ALTER TABLE distributed_test_vector ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
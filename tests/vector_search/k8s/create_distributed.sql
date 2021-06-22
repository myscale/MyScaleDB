CREATE TABLE distributed_test_vector ON CLUSTER
{cluster}
(
    id UInt64, vector FixedArray(Float32, 3)
) ENGINE = Distributed({cluster}, default, test_vector_local, rand());
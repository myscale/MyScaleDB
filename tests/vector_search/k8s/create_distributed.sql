CREATE TABLE distributed_test_vector ON CLUSTER
{cluster}
(
    id UInt64, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3
) ENGINE = Distributed({cluster}, default, test_vector_local, rand());
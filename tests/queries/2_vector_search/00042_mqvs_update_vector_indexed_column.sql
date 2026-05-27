-- Tags: no-parallel

SELECT 'update vector-indexed column is rejected';

DROP TABLE IF EXISTS test_update_vector_indexed_column SYNC;

CREATE TABLE test_update_vector_indexed_column
(
    id UInt64,
    vector Array(Float32),
    CONSTRAINT vector_len CHECK length(vector) = 3
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test_update_vector_indexed_column
SELECT number, [number, number, number]
FROM numbers(10);

ALTER TABLE test_update_vector_indexed_column ADD VECTOR INDEX v1 vector TYPE FLAT;

ALTER TABLE test_update_vector_indexed_column
UPDATE vector = [10000.0, 10000.0, 10000.0]
WHERE id = 0
SETTINGS mutations_sync = 2; -- { serverError QUERY_NOT_ALLOWED }

DROP TABLE IF EXISTS test_update_vector_indexed_column SYNC;

-- Tags: no-parallel

DROP TABLE IF EXISTS test_ttl_with_vector_index SYNC;
CREATE TABLE test_ttl_with_vector_index(
    date DateTime,
    id Float32, 
    vector Array(Float32),
    VECTOR INDEX vec_ind vector TYPE FLAT,
    CONSTRAINT check_length CHECK length(vector) = 3
)
ENGINE MergeTree()
ORDER BY id
Settings merge_with_ttl_timeout=5;

INSERT INTO test_ttl_with_vector_index SELECT
    toDateTime(now() + number * 5),
    number,
    [number, number, number]
FROM numbers(100);

INSERT INTO test_ttl_with_vector_index SELECT
    toDateTime(now() + number * 5),
    number,
    [number, number, number]
FROM numbers(100);

-- decouple part
OPTIMIZE TABLE test_ttl_with_vector_index final;

ALTER TABLE test_ttl_with_vector_index MODIFY TTL date + toIntervalSecond(1);

-- wait the next TTL DELETE period
SELECT sleep(1.99)+sleep(1.98)+sleep(1.0);

SELECT
    id,
    distance(vector, [0., 0., 0.]) AS dist
FROM test_ttl_with_vector_index
WHERE id < 50
ORDER BY dist ASC
LIMIT 6;

-- wait the next TTL DELETE period
SELECT sleep(1.99)+sleep(1.98)+sleep(1.0);

SELECT
    id,
    distance(vector, [0., 0., 0.]) AS dist
FROM test_ttl_with_vector_index
WHERE id < 50
ORDER BY dist ASC
LIMIT 6;

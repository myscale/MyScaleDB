
-- Tags: no-parallel

DROP TABLE IF EXISTS test_vector;
CREATE TABLE test_vector
(
    id    UInt32,
    data  Array(Float32),
    date  Date,
    label Enum8('person' = 1, 'building' = 2, 'animal' = 3),
    CONSTRAINT check_length CHECK length(data) = 3
)
engine = MergeTree PRIMARY KEY id settings min_bytes_for_wide_part=10485760;

INSERT INTO test_vector SELECT number, [number,number,number], '2022-12-30', 'person' FROM numbers(1000);
INSERT INTO test_vector SELECT number+1000, [number,number,number], '2022-12-29', 'animal' FROM numbers(1000);
INSERT INTO test_vector SELECT number+2000, [number,number,number], '2022-12-28', 'building' FROM numbers(1000);

ALTER TABLE test_vector ADD VECTOR INDEX vector_idx data TYPE IVFFLAT;

SELECT sleep(2);

SELECT 'explain syntax for sql w/o vector search';
EXPLAIN SYNTAX SELECT id FROM test_vector WHERE toYear(date) >= 2000 AND label = 'animal';

SELECT 'explain syntax for sql with vector search';
EXPLAIN SYNTAX SELECT id, date, label, distance(data, [0,1.0,2.0]) as dist
FROM test_vector
WHERE toYear(date) >= 2000 AND label = 'animal'
order by dist
limit 10;

SELECT id, date, label, distance(data, [0,1.0,2.0]) as dist
FROM test_vector
WHERE toYear(date) >= 2000 AND label = 'animal'
order by dist
limit 10;

SELECT 'explain syntax for sql with vector search and dist in where conditions';
EXPLAIN SYNTAX SELECT id, date, label, distance(data, [0,1.0,2.0]) as dist
FROM test_vector
WHERE toYear(date) >= 2000 AND label = 'animal' AND dist < 10
order by dist
limit 10;

SELECT id, date, label, distance(data, [0,1.0,2.0]) as dist
FROM test_vector
WHERE toYear(date) >= 2000 AND label = 'animal' AND dist < 10
order by dist
limit 10;

SELECT 'set optimize_move_to_prewhere_for_vector_search = 0';
SET optimize_move_to_prewhere_for_vector_search=0;
EXPLAIN SYNTAX SELECT id, date, label, distance(data, [0,1.0,2.0]) as dist
FROM test_vector
WHERE toYear(date) >= 2000 AND label = 'animal'
order by dist
limit 10;

DROP TABLE test_vector;
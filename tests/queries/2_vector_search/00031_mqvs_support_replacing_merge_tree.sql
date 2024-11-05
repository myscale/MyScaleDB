-- Tags: no-parallel

SET enable_brute_force_vector_search=1;

DROP TABLE IF EXISTS replacing_test SYNC;
CREATE TABLE replacing_test(
    id Float32, 
    vector Array(Float32),
    date Date,
    CONSTRAINT check_length CHECK length(vector) = 3
    ) engine ReplacingMergeTree
    ORDER BY id;

ALTER TABLE replacing_test ADD VECTOR INDEX mstg vector TYPE MSTG;

INSERT INTO replacing_test SELECT
    number,
    [number + 4, number + 4, number + 4],
    toDate('2023-04-01', 'UTC')
FROM numbers(10000);

INSERT INTO replacing_test SELECT
    number,
    [number + 3, number + 3, number + 3],
    toDate('2023-03-01', 'UTC')
FROM numbers(10000);

SYSTEM WAIT BUILDING VECTOR INDICES replacing_test;

OPTIMIZE TABLE replacing_test FINAL;

SYSTEM WAIT BUILDING VECTOR INDICES replacing_test;

SELECT
    id,
    date,
    distance(vector, [1., 2., 3.]) AS dist
FROM replacing_test
ORDER BY dist ASC
LIMIT 10;

delete from replacing_test where id < 10;

SELECT
    id,
    date,
    distance(vector, [1., 2., 3.]) AS dist
FROM replacing_test
ORDER BY dist ASC
LIMIT 10;

DROP TABLE IF EXISTS replacing_test SYNC;
CREATE TABLE replacing_test(
    id Float32, 
    vector Array(Float32),
    date Date,
    ver UInt8,
    CONSTRAINT check_length CHECK length(vector) = 3
    ) engine ReplacingMergeTree(ver)
    ORDER BY id;

ALTER TABLE replacing_test ADD VECTOR INDEX mstg vector TYPE MSTG;

INSERT INTO replacing_test SELECT
    number,
    [number + 4, number + 4, number + 4],
    toDate('2023-04-01', 'UTC'),
    number%2
FROM numbers(10000);

INSERT INTO replacing_test SELECT
    number,
    [number + 3, number + 3, number + 3],
    toDate('2023-03-01', 'UTC'),
    (number+1)%2
FROM numbers(10000);

SYSTEM WAIT BUILDING VECTOR INDICES replacing_test;

OPTIMIZE TABLE replacing_test FINAL;

SYSTEM WAIT BUILDING VECTOR INDICES replacing_test;

SELECT
    id,
    ver,
    date,
    distance(vector, [1., 2., 3.]) AS dist
FROM replacing_test
ORDER BY dist ASC
LIMIT 10;
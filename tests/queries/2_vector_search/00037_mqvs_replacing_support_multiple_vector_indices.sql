-- Tags: no-parallel

DROP TABLE IF EXISTS replacing_test_multi SYNC;
CREATE TABLE replacing_test_multi(
    id Float32, 
    vector Array(Float32),
    v2  Array(Float32),
    date Date,
    CONSTRAINT check_length CHECK length(vector) = 3,
    CONSTRAINT check_length CHECK length(v2) = 4
    ) engine ReplacingMergeTree
    ORDER BY id SETTINGS enable_rebuild_for_decouple=false, min_bytes_to_build_vector_index=16000;

ALTER TABLE replacing_test_multi ADD VECTOR INDEX vector_mstg vector TYPE MSTG;
ALTER TABLE replacing_test_multi ADD VECTOR INDEX v2_mstg v2 TYPE MSTG;

INSERT INTO replacing_test_multi SELECT
    number,
    [number + 4, number + 4, number + 4],
    [number, number, number, number],
    toDate('2023-04-01', 'UTC')
FROM numbers(6000);

INSERT INTO replacing_test_multi SELECT
    number,
    [number + 3, number + 3, number + 3],
    [number + 5, number + 5, number + 5, number + 5],
    toDate('2023-03-01', 'UTC')
FROM numbers(6000);

SYSTEM WAIT BUILDING VECTOR INDICES replacing_test_multi;

SELECT 'Vector index build status';
SELECT name, type, expr, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'replacing_test_multi';

OPTIMIZE TABLE replacing_test_multi FINAL;

SELECT 'Select on vector column in replacing merge tree with decouple part';
SELECT id, date, distance(vector, [1., 2., 3.]) AS dist FROM replacing_test_multi ORDER BY dist ASC LIMIT 10;

SELECT 'Select on v2 column in replacing merge tree with decouple part';
SELECT id, date, distance(v2, [8., 8., 8., 8.0]) AS dist FROM replacing_test_multi ORDER BY dist, id ASC LIMIT 10;

DROP TABLE IF EXISTS replacing_test_multi SYNC;

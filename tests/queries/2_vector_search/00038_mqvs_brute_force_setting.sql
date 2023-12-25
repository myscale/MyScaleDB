DROP TABLE IF EXISTS test_brute_force_setting SYNC;

SELECT 'min_rows_to_build_vector_index = 0';
CREATE TABLE test_brute_force_setting
(
    id    UInt32,
    vector  Array(Float32),
    CONSTRAINT check_length CHECK length(vector) = 3
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_rows_to_build_vector_index = 0;

INSERT INTO test_brute_force_setting SELECT number, [number,number,number] FROM numbers(100);

SELECT 'enable_brute_force_vector_search = 0';
SELECT id, distance(vector, [1.0, 1.0, 1.0]) AS dist FROM test_brute_force_setting ORDER BY dist, id LIMIT 5 SETTINGS enable_brute_force_vector_search = 0;

SELECT 'enable_brute_force_vector_search = 1';
SELECT id, distance(vector, [1.0, 1.0, 1.0]) AS dist FROM test_brute_force_setting ORDER BY dist, id LIMIT 5 SETTINGS enable_brute_force_vector_search = 1;

SELECT 'small part';
ALTER TABLE test_brute_force_setting MODIFY SETTING min_rows_to_build_vector_index = 1000;

SELECT 'enable_brute_force_vector_search = 0';
SELECT id, distance(vector, [1.0, 1.0, 1.0]) AS dist FROM test_brute_force_setting ORDER BY dist, id LIMIT 5 SETTINGS enable_brute_force_vector_search = 0;

SELECT 'enable_brute_force_vector_search = 1';
SELECT id, distance(vector, [1.0, 1.0, 1.0]) AS dist FROM test_brute_force_setting ORDER BY dist, id LIMIT 5 SETTINGS enable_brute_force_vector_search = 1;

DROP TABLE IF EXISTS test_brute_force_setting SYNC;

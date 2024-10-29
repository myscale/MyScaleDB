-- Tags: no-parallel

DROP TABLE IF EXISTS test_sparse_search_feature;
CREATE TABLE test_sparse_search_feature(
    `id` UInt64,
    `data` Map(UInt32, Float32)
) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_sparse_search_feature (id, data) VALUES (1, {1: 7.01, 2: 3.02, 3: 8.03}), (2, {1: 4.0, 3: 5.0, 5: 6.0}), (3, {4: 7.0, 7: 8.0}), (4, {1: 1.0, 2: 2.0, 3: 3.0}), (5, {2: 2.5, 4: 4.5}), (6, {1: 1.1, 3: 3.3, 5: 5.5, 7: 7.7}), (7, {2: 2.2, 6: 6.6}), (8, {1: 1.2, 2: 2.4, 3: 3.6, 4: 4.8}), (9, {5: 5.1, 6: 6.2, 7: 7.3}), (10, {1: 1.3, 3: 3.9, 5: 5.7, 7: 7.1, 9: 9.9});

-- Add sparse index
ALTER TABLE test_sparse_search_feature ADD INDEX sparse_idx data TYPE sparse;
ALTER TABLE test_sparse_search_feature MATERIALIZE INDEX sparse_idx;
SELECT sleep(1);

SELECT 'Sparse Search with Sparse Index';
SELECT id, SparseSearch(data, map(1, 0.4, 3, 1.6, 5, 2.1)) as score
FROM test_sparse_search_feature
ORDER BY score DESC
LIMIT 10;

-- ADD a new part
SYSTEM STOP MERGES test_sparse_search_feature;
INSERT INTO test_sparse_search_feature (id, data) VALUES (11, {2: 5.5, 4: 6.6, 6: 7.7}), (12, {1: 3.3, 3: 4.4, 5: 5.5}), (13, {2: 2.2, 4: 4.4, 8: 8.8}), (14, {3: 3.3, 6: 6.6, 9: 9.9}), (15, {1: 1.1, 5: 5.5, 7: 7.7}), (16, {2: 2.2, 3: 3.3, 8: 8.8}), (17, {4: 4.4, 5: 5.5, 9: 9.9}), (18, {1: 1.5, 6: 6.5, 7: 7.5}), (19, {3: 3.7, 4: 4.7, 8: 8.7}), (20, {2: 2.9, 5: 5.9, 9: 9.9});

SELECT 'Sparse Search on multiple parts';
SELECT id, SparseSearch('search_mode=brute_force')(data, mapFromArrays([1, 3, 5], [0.4, 1.6, 2.1])) as score
FROM test_sparse_search_feature
ORDER BY score DESC
LIMIT 20;

-- TODO: filter search and LWD
DROP TABLE IF EXISTS test_sparse_search_feature;


SET enable_brute_force_vector_search = 1;
SET allow_experimental_inverted_index = 1;

DROP TABLE IF EXISTS test_vector;
CREATE TABLE test_vector(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) engine MergeTree primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1000;
INSERT INTO test_vector SELECT number, [number, number, number] FROM numbers(100);

SELECT 'distance + limit 5,5';
SELECT id, distance(vector, [1.0, 1, 1]) as d FROM test_vector order by d limit 5,5;

SELECT 'distance + limit 5 offset 3';
SELECT id, distance(vector, [1.0, 1, 1]) as d FROM test_vector order by d limit 5 offset 3;

SELECT 'batch_distance + limit 5 offset 3 by dist.1';
SELECT id, batch_distance(vector, [[1.0, 1, 1], [50.0,50,50]]) as d FROM test_vector order by d.1, d.2 limit 5 offset 3 by d.1;

DROP TABLE IF EXISTS test_vector_inverted;
CREATE TABLE test_vector_inverted
(
    id UInt64,
    vector Array(Float32),
    doc String,
    INDEX inv_idx(doc) TYPE fts GRANULARITY 1,
    CONSTRAINT vector_len CHECK length(vector) = 3
)
ENGINE = MergeTree ORDER BY id settings index_granularity=2;

INSERT INTO test_vector_inverted SELECT number, [number,number,number], if(number < 6, 'myscale is a vector database', 'text search and hybrid search') FROM numbers(100);

SELECT 'text search + limit 1,5';
SELECT id, textsearch(doc, 'myscale') as score FROM test_vector_inverted ORDER BY score DESC LIMIT 1,5;

SELECT 'text search + limit 5 offset 1';
SELECT id, textsearch(doc, 'myscale') as score FROM test_vector_inverted ORDER BY score DESC LIMIT 5 offset 1;

SELECT 'hybrid search + limit 1,5';
SELECT id, hybridsearch('fusion_type=rsf')(vector, doc, [1.0,1,1], 'myscale') as score
FROM test_vector_inverted
ORDER BY score DESC, id LIMIT 1,5;

SELECT 'hybrid search + limit 5 offset 1';
SELECT id, hybridsearch('fusion_type=rsf')(vector, doc, [1.0,1,1], 'myscale') as score
FROM test_vector_inverted
ORDER BY score DESC, id LIMIT 5 offset 1;

SELECT id, textsearch(doc, 'myscale') as score FROM test_vector_inverted ORDER BY score DESC LIMIT 1,5 SETTINGS max_search_result_window=3; -- { serverError BAD_ARGUMENTS } 

DROP TABLE IF EXISTS test_vector;
DROP TABLE IF EXISTS test_vector_inverted;

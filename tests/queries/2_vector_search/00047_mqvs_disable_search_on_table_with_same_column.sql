
SET enable_brute_force_vector_search = 1;
SET allow_experimental_inverted_index = 1;

DROP TABLE IF EXISTS test_vector_same SYNC;
CREATE TABLE test_vector_same
(
    distance_func UInt32,
    vector  Array(Float32),
    CONSTRAINT check_length CHECK length(vector) = 3
)
engine = MergeTree PRIMARY KEY distance_func;

INSERT INTO test_vector_same SELECT number, [number,number,number] FROM numbers(10);

SELECT 'distance function';
SELECT distance_func, distance(vector, [1.0, 2.0, 3.0]) AS d FROM test_vector_same ORDER BY d LIMIT 5; -- {serverError SYNTAX_ERROR}

SELECT 'select distance_func from table';
SELECT distance_func FROM test_vector_same LIMIT 1;

SELECT 'select distance_func + batch_distance() function';
SELECT distance_func, batch_distance(vector,[[1.0,1,1], [2.0,2,2]]) AS d FROM test_vector_same ORDER BY d.1, d.2 LIMIT 2 BY d.1;

DROP TABLE IF EXISTS test_vector_same SYNC;

DROP TABLE IF EXISTS test_inverted_same;
CREATE TABLE test_inverted_same
(
    textsearch_func UInt64,
    doc String,
    INDEX inv_idx(doc) TYPE fts GRANULARITY 1
)
ENGINE = MergeTree ORDER BY textsearch_func settings index_granularity=2;

INSERT INTO test_inverted_same SELECT number, if(number < 6, 'myscale is a vector database', 'text search and hybrid search') FROM numbers(100);

SELECT 'textsearch function';
SELECT textsearch_func, textsearch(doc, 'myscale') as score FROM test_inverted_same ORDER BY score DESC LIMIT 1; -- {serverError SYNTAX_ERROR}

DROP TABLE IF EXISTS test_inverted_same;

DROP TABLE IF EXISTS test_vector_inverted_same;
CREATE TABLE test_vector_inverted_same
(
    hybridsearch_func UInt64,
    vector Array(Float32),
    doc String,
    INDEX inv_idx(doc) TYPE fts GRANULARITY 1,
    CONSTRAINT vector_len CHECK length(vector) = 3
)
ENGINE = MergeTree ORDER BY hybridsearch_func settings index_granularity=2;

INSERT INTO test_vector_inverted_same SELECT number, [number,number,number], if(number == 1, 'myscale is a vector database', 'text search and hybrid search') FROM numbers(100);

SELECT 'hybridsearch function';
SELECT hybridsearch_func, hybridsearch('fusion_type=rsf')(vector, doc, [1.0,1,1], 'myscale') as score
FROM test_vector_inverted_same ORDER BY score DESC, id LIMIT 1; -- {serverError SYNTAX_ERROR}

SELECT 'select hybridsearch_func + textsearch() function';
SELECT hybridsearch_func, textsearch(doc, 'myscale') as score FROM test_vector_inverted_same ORDER BY score DESC LIMIT 1;
SELECT 'select hybridsearch_func + distance() function';
SELECT hybridsearch_func, distance(vector, [1.0,1,1]) as dist FROM test_vector_inverted_same ORDER BY dist LIMIT 1;

DROP TABLE IF EXISTS test_vector_inverted_same;


SET allow_experimental_inverted_index = 1;
SET enable_brute_force_vector_search = 1;

DROP TABLE IF EXISTS test_vector_inverted;
CREATE TABLE test_vector_inverted
(
    id UInt64,
    vector Array(Float32),
    doc String,
    INDEX inv_idx(doc) TYPE fts('{"doc":{"tokenizer":{"type":"stem", "stop_word_filters":["english"]}}}') GRANULARITY 1,
    CONSTRAINT vector_len CHECK length(vector) = 3
)
ENGINE = MergeTree ORDER BY id settings index_granularity=2;

INSERT INTO test_vector_inverted VALUES(0, arrayWithConstant(3, 0), 'MyScale is an advanced SQL vector database built on the ClickHouse columnar database, leveraging high-performance, high-data-density vector index algorithms and includes full-text search capabilities.'), (1, arrayWithConstant(3,1), 'Through extensive research and optimization of its retrieval and storage engines, MyScale efficiently supports combined SQL, vector, and full-text search queries, offering robust data management capabilities, high performance, and cost-effectiveness.'), (2, arrayWithConstant(3,2), 'The FINAL keyword is a query modifier in MyScale that is crucial for ensuring fully merged data results in queries'), (3, arrayWithConstant(3,3), 'duplicate data with the same sorting key values are not allowed to coexist'), (4, arrayWithConstant(3, 4), 'MyScale has been optimized for efficient storage and querying of diverse data types, including strings, JSON, spatial, and time series data'), (5, arrayWithConstant(3,5), 'Myscale also excels in vector search capabilities and has recently introduced powerful inverted index and text search functionalities'), (6, arrayWithConstant(3,6), 'The ReplacingMergeTree table engine in MyScale effectively handles data updates but can leave duplicates in unmerged partitions'), (7, arrayWithConstant(3,7), 'Therefore, using the FINAL keyword is highly recommended to maintain data integrity and consistency');

-- invalid cases
SELECT id, textsearch('enable_nlq=1abc')(doc, 'myscale') as score FROM test_vector_inverted ORDER BY score DESC LIMIT 5; -- { serverError BAD_ARGUMENTS }
SELECT id, textsearch('enable_nlq=false1')(doc, 'myscale') as score FROM test_vector_inverted ORDER BY score DESC LIMIT 5; -- { serverError BAD_ARGUMENTS }

SELECT 'text search with natural language query enabled, operator=OR (default)';
SELECT id, textsearch('enable_nlq=true')(doc, 'MyScale AND (SQL OR FINAL)') as score FROM test_vector_inverted ORDER BY score DESC LIMIT 5;

SELECT 'text search with natural language query enabled(default), operator=OR';
SELECT id, textsearch('operator=OR')(doc, 'MyScale AND (SQL OR FINAL)') as score FROM test_vector_inverted ORDER BY score DESC LIMIT 5;

SELECT 'text search with natural language query enabled(default), operator=AND';
SELECT id, textsearch('operator=AND')(doc, 'MyScale AND (SQL OR FINAL)') as score FROM test_vector_inverted ORDER BY score DESC LIMIT 5;

SELECT 'text search with natural language query disabled, operator=OR (default)';
SELECT id, textsearch('enable_nlq=false')(doc, 'MyScale AND (SQL OR FINAL)') as score FROM test_vector_inverted ORDER BY score DESC LIMIT 5;

SELECT 'text search with natural language query disabled, operator=AND';
SELECT id, textsearch('enable_nlq=false', 'operator=AND')(doc, 'MyScale AND (SQL OR FINAL)') as score FROM test_vector_inverted ORDER BY score DESC LIMIT 5;

SELECT 'hybrid search with natural language query enabled';
SELECT id, hybridsearch('fusion_type=rsf', 'enable_nlq=1')(vector, doc, [1.0,1,1], 'MyScale AND (SQL OR FINAL)') as score FROM test_vector_inverted ORDER BY score DESC LIMIT 5;

SELECT 'hybrid search with natural language query disabled, operator=OR (default)';
SELECT id, hybridsearch('fusion_type=rsf', 'enable_nlq=0')(vector, doc, [1.0,1,1], 'MyScale AND (SQL OR FINAL)') as score FROM test_vector_inverted ORDER BY score DESC LIMIT 5;

SELECT 'hybrid search with natural language query disabled, operator=AND';
SELECT id, hybridsearch('fusion_type=rsf', 'enable_nlq=0', 'operator=AND')(vector, doc, [1.0,1,1], 'MyScale AND (SQL OR FINAL)') as score FROM test_vector_inverted ORDER BY score DESC LIMIT 5;

DROP TABLE IF EXISTS test_vector_inverted;

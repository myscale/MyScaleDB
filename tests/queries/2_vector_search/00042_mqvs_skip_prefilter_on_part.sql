
DROP TABLE IF EXISTS test_vector_skip_prefilter;
CREATE TABLE test_vector_skip_prefilter
(
    collection_type LowCardinality(String),
    collection_id String,
    doc_id UInt64,
    all_vector Array(Float32),
    CONSTRAINT all_vector_len CHECK length(all_vector) = 3
)
ENGINE = MergeTree PARTITION BY collection_type ORDER BY (collection_id, doc_id);

INSERT INTO test_vector_skip_prefilter SELECT 's2', 's2', number, [number,number,number] FROM numbers(200) WHERE number%2 = 0;
INSERT INTO test_vector_skip_prefilter SELECT 'private', number%14, number, [number,number,number] FROM numbers(200) WHERE number%2 = 1;

SET enable_brute_force_vector_search = 1;
SET optimize_prefilter_in_search = 1;
SELECT 'Select with optimize_prefilter_in_search enabled';
SELECT doc_id FROM test_vector_skip_prefilter WHERE collection_type IN ['arxiv', 's2'] ORDER BY distance(all_vector,[1.0,1,1]) AS d, doc_id ASC limit 5;

SELECT 'Select with optimize_prefilter_in_search enabled and OR condition';
SELECT doc_id FROM test_vector_skip_prefilter WHERE collection_type IN ['arxiv', 's2'] OR (collection_type = 'private' AND collection_id = '1') ORDER BY distance(all_vector,[1.0,1,1]) AS d, doc_id ASC limit 5;

SELECT 'Select with optimize_prefilter_in_search disabled';
SET optimize_prefilter_in_search = 0;
SELECT doc_id FROM test_vector_skip_prefilter WHERE collection_type IN ['arxiv', 's2'] ORDER BY distance(all_vector,[1.0,1,1]) AS d, doc_id ASC limit 5;

DROP TABLE IF EXISTS test_vector_skip_prefilter;

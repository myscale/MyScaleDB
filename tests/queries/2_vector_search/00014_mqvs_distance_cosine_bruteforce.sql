-- Tags: no-parallel

DROP TABLE IF EXISTS test_vector SYNC;

CREATE TABLE test_vector(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) engine=MergeTree primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1, vector_search_metric_type='Cosine';
INSERT INTO test_vector SELECT number, [number, number + 3, number + 1] FROM numbers(1000);

SELECT id, distance(vector, [8., 11, 9]) AS d FROM test_vector ORDER BY d LIMIT 5;

DROP TABLE test_vector sync;

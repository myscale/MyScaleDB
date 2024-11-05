-- Tags: no-parallel

DROP TABLE IF EXISTS test_mstg SYNC;

CREATE TABLE test_mstg(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 8) engine=MergeTree primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1, min_bytes_to_build_vector_index=40000;
INSERT INTO test_mstg SELECT number, [number, number + 7, number + 6, number + 5, number + 4, number + 3, number + 2, number + 1] FROM numbers(1000);
OPTIMIZE TABLE test_mstg FINAL;
ALTER TABLE test_mstg ADD VECTOR INDEX v1 vector TYPE MSTG('metric_type=Cosine');
SYSTEM WAIT BUILDING VECTOR INDICES test_mstg;
SELECT table, name, expr, status, latest_failed_part, latest_fail_reason FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_mstg';
SELECT id, distance(vector, [8., 15, 14, 13, 12, 11, 10, 9]) AS d FROM test_mstg ORDER BY d LIMIT 5;
DETACH TABLE test_mstg;
ATTACH TABLE test_mstg;
SELECT id, distance(vector, [8., 15, 14, 13, 12, 11, 10, 9]) AS d FROM test_mstg ORDER BY d LIMIT 5;
DROP TABLE test_mstg sync;

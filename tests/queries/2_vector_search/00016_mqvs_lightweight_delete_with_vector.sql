-- Tags: no-parallel

DROP TABLE IF EXISTS test_vector;
CREATE TABLE test_vector(id Float32, vector FixedArray(Float32, 3)) engine MergeTree primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=0, min_bytes_for_wide_part=0;
INSERT INTO test_vector SELECT number, [number, number, number] FROM numbers(2100);
ALTER TABLE test_vector ADD VECTOR INDEX v1 vector TYPE HNSWFLAT;

SELECT sleep(2);

set allow_experimental_lightweight_delete=1;
set mutations_sync=1;

delete from test_vector where id = 2;

SELECT sleep(1);

SELECT id, vector, distance('topK=10')(vector, [0.1, 0.1, 0.1]) FROM test_vector;

drop table test_vector;

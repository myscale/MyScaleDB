DROP TABLE IF EXISTS test_pk_cache;
CREATE TABLE test_pk_cache(id UInt32, vector FixedArray(Float32, 3)) engine MergeTree primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1000, enable_primary_key_cache=true;
INSERT INTO test_pk_cache SELECT number, [number, number, number] FROM numbers(2100);

ALTER TABLE test_pk_cache ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
SELECT sleep(2);

SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache;

set allow_experimental_lightweight_delete=1;
set mutations_sync=1;

delete from test_pk_cache where id = 3;
SELECT sleep(1);

SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache;

drop table test_pk_cache;

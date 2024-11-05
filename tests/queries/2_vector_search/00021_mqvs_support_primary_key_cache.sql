DROP TABLE IF EXISTS test_pk_cache;
CREATE TABLE test_pk_cache(id UInt32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) engine MergeTree primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1000, enable_primary_key_cache=true;
INSERT INTO test_pk_cache SELECT number, [number, number, number] FROM numbers(2100);

ALTER TABLE test_pk_cache ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
SYSTEM WAIT BUILDING VECTOR INDICES test_pk_cache;

SELECT id, distance(vector, [0.1, 0.1, 0.1]) as d FROM test_pk_cache order by d limit 5;

set allow_experimental_lightweight_delete=1;
set mutations_sync=1;

delete from test_pk_cache where id = 3;

SELECT id, distance(vector, [0.1, 0.1, 0.1]) as d FROM test_pk_cache order by d limit 5;

drop table test_pk_cache;

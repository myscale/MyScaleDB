-- Tags: no-parallel

DROP TABLE IF EXISTS test_drop_index;
CREATE TABLE test_drop_index(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) engine MergeTree primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1000;
INSERT INTO test_drop_index SELECT number, [number, number, number] FROM numbers(2100);
ALTER TABLE test_drop_index ADD VECTOR INDEX v1 vector TYPE HNSWFLAT;

SYSTEM WAIT BUILDING VECTOR INDICES test_drop_index;

select table, name, type, expr, status from system.vector_indices where database = currentDatabase() and table = 'test_drop_index';

ALTER TABLE test_drop_index DROP VECTOR INDEX v1;

select '-- Empty result, no vector index';
select table, name, type, expr, status from system.vector_indices where database = currentDatabase() and table = 'test_drop_index';

select '-- Create a new vector index with same name but different type';
ALTER TABLE test_drop_index ADD VECTOR INDEX v1 vector TYPE IVFFlat;

SYSTEM WAIT BUILDING VECTOR INDICES test_drop_index;

select table, name, type, expr, status from system.vector_indices where database = currentDatabase() and table = 'test_drop_index';

drop table test_drop_index;

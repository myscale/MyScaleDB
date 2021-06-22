-- Tags: no-parallel

drop table if exists test_vector;
CREATE TABLE test_vector
(
   id UInt64,
   vector Array(Float32),
   label String,
   CONSTRAINT vector_len CHECK length(vector) = 3
)
ENGINE = MergeTree PRIMARY KEY id;

create vector index if not exists i_h on test_vector vector TYPE FLAT;
SELECT table, name, type, expr, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_vector' FORMAT Vertical;
drop vector index if exists i_h on test_vector;
SELECT table, name, type, expr, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_vector' FORMAT Vertical;

create vector index i_h on test_vector vector TYPE FLAT;
drop vector index i_h on test_vector; 

create index i_a on test_vector(id) TYPE minmax GRANULARITY 4;
create index if not exists i_a on test_vector(id) TYPE minmax GRANULARITY 2;

create index i_label on test_vector(label) TYPE bloom_filter GRANULARITY 2;

show create table test_vector;
select table, name, type, expr, granularity from system.data_skipping_indices where database = currentDatabase() and table = 'test_vector'; 


drop index i_a on test_vector;
drop index if exists i_a on test_vector;

select table, name, type, expr, granularity from system.data_skipping_indices where database = currentDatabase() and table = 'test_vector'; 

drop table test_vector;

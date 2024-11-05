-- Tags: no-parallel

drop table if exists test_vector;
create table test_vector
(
    id    UInt32,
    data  Array(Float32),
    date  Date,
    label Enum8('person' = 1, 'building' = 2, 'animal' = 3),
    CONSTRAINT data_len CHECK length(data) = 3
)
engine = MergeTree PRIMARY KEY id;

INSERT INTO test_vector SELECT number, range(3), '2019-12-30', 'person' FROM numbers(1000);
INSERT INTO test_vector SELECT number+1000, range(3), '2022-12-29', 'animal' FROM numbers(1000);
INSERT INTO test_vector SELECT number+2000, range(3), '2018-12-28', 'building' FROM numbers(1000);

optimize table test_vector final;

alter table test_vector add vector index vector_idx data type IVFFLAT;
SYSTEM WAIT BUILDING VECTOR INDICES test_vector;
select table, name, type, status from system.vector_indices where database = currentDatabase() and table = 'test_vector';

select id, date, label, distance(data, [0,1.0,2.0]) as dist from test_vector where toYear(date) >= 2020 and label = 'animal' order by dist limit 10;

-- Tags: no-parallel
set two_stage_search_option=2, enable_brute_force_vector_search=1;
DROP TABLE IF EXISTS test_vector_two_stage SYNC;
CREATE TABLE test_vector_two_stage
(
    id    UInt32,
    vector  Array(Float32),
    CONSTRAINT check_length CHECK length(vector) = 16
)
engine = MergeTree
ORDER BY id
SETTINGS min_bytes_to_build_vector_index=0;

ALTER TABLE test_vector_two_stage ADD VECTOR INDEX vec_ind vector TYPE MSTG;

INSERT INTO test_vector_two_stage SELECT number, [number,number,number,number,number,number,number,number,number,number,number,number,number,number,number,number] FROM numbers(1001) where number != 1;

SELECT 'two stage search with MSTG type and min_bytes_to_build_vector_index=0';
SYSTEM WAIT BUILDING VECTOR INDICES test_vector_two_stage;

SELECT id, distance(vector,[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]) AS dist FROM test_vector_two_stage WHERE id < 11 ORDER BY dist, id LIMIT 10;

SELECT 'disable two stage search';
SELECT id, distance(vector,[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]) AS dist FROM test_vector_two_stage ORDER BY dist, id LIMIT 10 settings two_stage_search_option=0;

DROP TABLE IF EXISTS test_vector_two_stage_mix SYNC;
CREATE TABLE test_vector_two_stage_mix
(
    id    UInt32,
    vector  Array(Float32),
    CONSTRAINT check_length CHECK length(vector) = 16
)
engine = MergeTree
ORDER BY id
SETTINGS min_bytes_to_build_vector_index=65536;

ALTER TABLE test_vector_two_stage_mix ADD VECTOR INDEX vec_ind vector TYPE MSTG;

INSERT INTO test_vector_two_stage_mix SELECT number, [number,number,number,number,number,number,number,number,number,number,number,number,number,number,number,number] FROM numbers(1000);
INSERT INTO test_vector_two_stage_mix SELECT number, [number,number,number,number,number,number,number,number,number,number,number,number,number,number,number,number] FROM numbers(1001, 2000);

SELECT 'two stage search with MSTG type and min_bytes_to_build_vector_index=1024';
SYSTEM WAIT BUILDING VECTOR INDICES test_vector_two_stage_mix;

SELECT id, distance(vector,[1000.0,1000.0,1000.0,1000.0,1000.0,1000.0,1000.0,1000.0,1000.0,1000.0,1000.0,1000.0,1000.0,1000.0,1000.0,1000.0]) AS dist FROM test_vector_two_stage_mix WHERE id between 990 and 1010 ORDER BY dist, id LIMIT 10;

DROP TABLE IF EXISTS test_vector_two_stage_decouple SYNC;
CREATE TABLE test_vector_two_stage_decouple
(
    id    UInt32,
    vector  Array(Float32),
    CONSTRAINT check_length CHECK length(vector) = 16
)
engine = MergeTree
ORDER BY id
SETTINGS enable_rebuild_for_decouple=false;

ALTER TABLE test_vector_two_stage_decouple ADD VECTOR INDEX vec_ind vector TYPE MSTG;

INSERT INTO test_vector_two_stage_decouple SELECT number, [number,number,number,number,number,number,number,number,number,number,number,number,number,number,number,number] FROM numbers(1000);
INSERT INTO test_vector_two_stage_decouple SELECT number, [number,number,number,number,number,number,number,number,number,number,number,number,number,number,number,number] FROM numbers(1001, 2000);

SELECT 'two stage search with MSTG type and decouple part';
SYSTEM WAIT BUILDING VECTOR INDICES test_vector_two_stage_decouple;

optimize table test_vector_two_stage_decouple final;

SELECT id, distance(vector,[1000.0,1000.0,1000.0,1000.0,1000.0,1000.0,1000.0,1000.0,1000.0,1000.0,1000.0,1000.0,1000.0,1000.0,1000.0,1000.0]) AS dist FROM test_vector_two_stage_decouple WHERE id between 995 and 1005 ORDER BY dist, id LIMIT 10;

SELECT 'two stage search with MSTG type and metric_type=IP';
DROP TABLE IF EXISTS test_vector_two_stage_ip SYNC;
CREATE TABLE test_vector_two_stage_ip
(
    id    UInt32,
    vector  Array(Float32),
    CONSTRAINT check_length CHECK length(vector) = 3
)
engine = MergeTree
ORDER BY id
SETTINGS min_bytes_to_build_vector_index=0;

ALTER TABLE test_vector_two_stage_ip ADD VECTOR INDEX vec_ind vector TYPE MSTG('metric_type=IP');

INSERT INTO test_vector_two_stage_ip SELECT number, [number,number,number] FROM numbers(40000) where number %4=0;
INSERT INTO test_vector_two_stage_ip SELECT number, [number,number,number] FROM numbers(40000) where number %4=1;
INSERT INTO test_vector_two_stage_ip SELECT number, [number,number,number] FROM numbers(40000) where number %4=2;
INSERT INTO test_vector_two_stage_ip SELECT number, [number,number,number] FROM numbers(40000) where number %4=3;

SYSTEM WAIT BUILDING VECTOR INDICES test_vector_two_stage_ip;

SELECT id, distance(vector,[1.0,1.0,1.0]) AS dist FROM test_vector_two_stage_ip ORDER BY dist DESC LIMIT 10;

DROP TABLE test_vector_two_stage_mix;
DROP TABLE test_vector_two_stage;
DROP TABLE test_vector_two_stage_decouple;
DROP TABLE test_vector_two_stage_ip;

DROP TABLE IF EXISTS test_vector;
CREATE TABLE test_vector(id Float32, vector FixedArray(Float32, 3)) engine MergeTree primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1, distable_rebuild_for_decouple=true,max_rows_for_slow_mode_single_vector_index_build = 10;
INSERT INTO test_vector SELECT number, [number, number, number] FROM numbers(100);
ALTER TABLE test_vector ADD VECTOR INDEX v1 vector TYPE HNSWFLAT;

SELECT sleep(3);

INSERT INTO test_vector SELECT number + 100, [number + 100, number + 100, number + 100] FROM numbers(100);
INSERT INTO test_vector SELECT number + 200, [number + 200, number + 200, number + 200] FROM numbers(100);

SELECT sleep(3);

SELECT id, vector, distance('topK=10')(vector, [0.1, 0.1, 0.1]) FROM test_vector;

set allow_experimental_lightweight_delete=1;
set mutations_sync=1;

delete from test_vector where id = 2;
delete from test_vector where id = 10;

SELECT id, vector, distance('topK=10')(vector, [0.1, 0.1, 0.1]) FROM test_vector;
SELECT id, vector, distance('topK=10')(vector, [0.1, 0.1, 0.1]) FROM test_vector prewhere id > 5;
optimize table test_vector final;

SELECT id, vector, distance('topK=10')(vector, [0.1, 0.1, 0.1]) FROM test_vector;
SELECT id, vector, distance('topK=10')(vector, [0.1, 0.1, 0.1]) FROM test_vector prewhere id > 5;

SELECT '--- lightweight delete on decoupled part';
delete from test_vector where id = 3;
delete from test_vector where id = 15;

select table, name, type, total_parts, status from system.vector_indices where database = currentDatabase() and table = 'test_vector';

SELECT id, vector, distance('topK=10')(vector, [0.1, 0.1, 0.1]) FROM test_vector;
SELECT id, vector, distance('topK=10')(vector, [0.1, 0.1, 0.1]) FROM test_vector prewhere id > 5;

drop table test_vector;

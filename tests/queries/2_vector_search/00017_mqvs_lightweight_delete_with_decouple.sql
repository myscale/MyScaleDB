-- Tags: no-parallel

DROP TABLE IF EXISTS test_vector;
CREATE TABLE test_vector(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) engine MergeTree primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1, disable_rebuild_for_decouple=true,max_rows_for_slow_mode_single_vector_index_build = 10;
INSERT INTO test_vector SELECT number, [number, number, number] FROM numbers(100);
ALTER TABLE test_vector ADD VECTOR INDEX v1 vector TYPE HNSWFLAT;

SELECT sleep(3);

INSERT INTO test_vector SELECT number + 100, [number + 100, number + 100, number + 100] FROM numbers(100);
INSERT INTO test_vector SELECT number + 200, [number + 200, number + 200, number + 200] FROM numbers(100);

SELECT sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95);
SELECT '--- Original topK result';
SELECT id, vector, distance(vector, [0.1, 0.1, 0.1]) as d FROM test_vector order by d limit 10;

set allow_experimental_lightweight_delete=1;
set mutations_sync=1;
SELECT '--- Lightweight delete on parts with vector index';
delete from test_vector where id = 2;
delete from test_vector where id = 10;
SELECT sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95);
SELECT '--- After lightweight, select from test_vector limit 10';
SELECT id, vector, distance(vector, [0.1, 0.1, 0.1]) as d FROM test_vector order by d limit 10;
SELECT '--- After lightweight, select from test_vector id>5 limit 10';
SELECT id, vector, distance(vector, [0.1, 0.1, 0.1]) as d FROM test_vector prewhere id > 5 order by d limit 10;

SELECT '--- Decoupled part when source parts contain lightweight delete';
optimize table test_vector final;
SELECT sleep(2);
SELECT '--- After optimize, select from test_vector limit 10';
SELECT id, vector, distance(vector, [0.1, 0.1, 0.1]) as d FROM test_vector order by d limit 10;
SELECT '--- After optimize, select from test_vector id>5 limit 10';
SELECT id, vector, distance(vector, [0.1, 0.1, 0.1]) as d FROM test_vector prewhere id > 5 order by d limit 10;

SELECT '--- lightweight delete on decoupled part';
delete from test_vector where id = 3;
delete from test_vector where id = 15;
SELECT sleep(1.99)+sleep(1.98);

select table, name, type, total_parts, status from system.vector_indices where database = currentDatabase() and table = 'test_vector';
SELECT '--- After lightweight, select from test_vector limit 10';
SELECT id, vector, distance(vector, [0.1, 0.1, 0.1]) as d FROM test_vector order by d limit 10;
SELECT '--- After lightweight, select from test_vector id>5 limit 10';
SELECT id, vector, distance(vector, [0.1, 0.1, 0.1]) as d FROM test_vector prewhere id > 5 order by d limit 10;

drop table test_vector;

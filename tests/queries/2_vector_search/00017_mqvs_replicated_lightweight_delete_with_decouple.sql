-- Tags: no-parallel

DROP TABLE IF EXISTS test_replicated_vector SYNC;
DROP TABLE IF EXISTS test_replicated_vector2 SYNC;
CREATE TABLE test_replicated_vector(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/mqvs_00017/vector', 'r1') primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1, disable_rebuild_for_decouple=true,max_rows_for_slow_mode_single_vector_index_build = 10;
CREATE TABLE test_replicated_vector2(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/mqvs_00017/vector', 'r2') primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1, disable_rebuild_for_decouple=true,max_rows_for_slow_mode_single_vector_index_build = 10;
INSERT INTO test_replicated_vector SELECT number, [number, number, number] FROM numbers(100);
ALTER TABLE test_replicated_vector ADD VECTOR INDEX v1 vector TYPE MSTG;

SELECT sleep(3);

INSERT INTO test_replicated_vector SELECT number + 100, [number + 100, number + 100, number + 100] FROM numbers(100);
INSERT INTO test_replicated_vector SELECT number + 200, [number + 200, number + 200, number + 200] FROM numbers(100);

SELECT sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95);

select table, name, type, total_parts, status from system.vector_indices where database = currentDatabase() and (table = 'test_replicated_vector' OR table = 'test_replicated_vector2');

SELECT '--- Original topK result';
SELECT id, vector, distance(vector, [0.1, 0.1, 0.1]) as d FROM test_replicated_vector order by d limit 10;

SELECT '--- Lightweight delete on parts with vector index';
set allow_experimental_lightweight_delete=1;
set mutations_sync=2;
delete from test_replicated_vector where id = 2;
delete from test_replicated_vector where id = 10;

SELECT sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95);

select table, name, type, total_parts, status from system.vector_indices where database = currentDatabase() and (table = 'test_replicated_vector' OR table = 'test_replicated_vector2');
SELECT '--- After lightweight, select from test_replicated_vector2 limit 10';
SELECT id, vector, distance(vector, [0.1, 0.1, 0.1]) as d FROM test_replicated_vector2 order by d limit 10;
SELECT '--- After lightweight, select from test_replicated_vector id>5 limit 10';
SELECT id, vector, distance(vector, [0.1, 0.1, 0.1]) as d FROM test_replicated_vector prewhere id > 5 order by d limit 10;

SELECT '--- Decoupled part when source parts contain lightweight delete';
optimize table test_replicated_vector final;
select sleep(2);
select table, name, type, total_parts, status from system.vector_indices where database = currentDatabase() and (table = 'test_replicated_vector' OR table = 'test_replicated_vector2');
SELECT '--- After optimize, select from test_replicated_vector limit 10';
SELECT id, vector, distance(vector, [0.1, 0.1, 0.1]) as d FROM test_replicated_vector order by d limit 10;
SELECT '--- After optimize, select from test_replicated_vector2 id>5 limit 10';
SELECT id, vector, distance(vector, [0.1, 0.1, 0.1]) as d FROM test_replicated_vector2 prewhere id > 5 order by d limit 10;

SELECT '--- Lightweight delete on decoupled part';
delete from test_replicated_vector where id = 3;
delete from test_replicated_vector where id = 15;

SELECT sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95);

select table, name, type, total_parts, status from system.vector_indices where database = currentDatabase() and (table = 'test_replicated_vector' OR table = 'test_replicated_vector2');
SELECT '--- After lightweight, select from test_replicated_vector2 limit 10';
SELECT id, vector, distance(vector, [0.1, 0.1, 0.1]) as d FROM test_replicated_vector2 order by d limit 10;
SELECT '--- After lightweight, select from test_replicated_vector id>5 limit 10';
SELECT id, vector, distance(vector, [0.1, 0.1, 0.1]) as d FROM test_replicated_vector prewhere id > 5 order by d limit 10;

DROP TABLE IF EXISTS test_replicated_vector SYNC;
DROP TABLE IF EXISTS test_replicated_vector2 SYNC;

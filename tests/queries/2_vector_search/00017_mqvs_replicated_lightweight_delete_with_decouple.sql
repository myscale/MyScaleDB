-- Tags: no-parallel

DROP TABLE IF EXISTS test_replicated_vector SYNC;
DROP TABLE IF EXISTS test_replicated_vector2 SYNC;
CREATE TABLE test_replicated_vector(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 32) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/mqvs_00017/vector', 'r1') primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1, enable_rebuild_for_decouple=false,max_rows_for_slow_mode_single_vector_index_build = 10;
CREATE TABLE test_replicated_vector2(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 32) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/mqvs_00017/vector', 'r2') primary key id SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1, enable_rebuild_for_decouple=false,max_rows_for_slow_mode_single_vector_index_build = 10;
INSERT INTO test_replicated_vector SELECT number as id, arrayMap(x -> 0.0008 * (number * 32 + x + 1) * (if(x % 2 = 0, -1, 1)), range(32)) as vector FROM numbers(1000);
ALTER TABLE test_replicated_vector ADD VECTOR INDEX v1 vector TYPE MSTG;

SYSTEM WAIT BUILDING VECTOR INDICES test_replicated_vector;

INSERT INTO test_replicated_vector SELECT number as id, arrayMap(x -> 0.0005 * (number * 32 + x + 1) * (if(x % 2 = 0, -1, 1)), range(32)) as vector FROM numbers(1000, 1000);
INSERT INTO test_replicated_vector SELECT number as id, arrayMap(x -> 0.0001 * (number * 32 + x + 1) * (if(x % 2 = 0, -1, 1)), range(32)) as vector FROM numbers(2000, 1000);

SYSTEM WAIT BUILDING VECTOR INDICES test_replicated_vector;
SYSTEM WAIT BUILDING VECTOR INDICES test_replicated_vector2;
select table, name, type, total_parts, status from system.vector_indices where database = currentDatabase() and (table = 'test_replicated_vector' OR table = 'test_replicated_vector2');

SELECT '--- Original topK result';
SELECT id, distance(vector, [0.01,0.02,0.03,0.04,0.05,0.06,0.07,0.08,0.09,0.1,0.11,0.12,0.13,0.14,0.15,0.16,0.17,0.18,0.19,0.2,0.21,0.22,0.23,0.24,0.25,0.26,0.27,0.28,0.29,0.3,0.31,0.32 ]) AS d FROM test_replicated_vector ORDER BY d LIMIT 10;


SELECT '--- Lightweight delete on parts with vector index';
set allow_experimental_lightweight_delete=1;
set mutations_sync=2;
delete from test_replicated_vector where id = 2;
delete from test_replicated_vector where id = 8;

SYSTEM WAIT BUILDING VECTOR INDICES test_replicated_vector;
select table, name, type, total_parts, status from system.vector_indices where database = currentDatabase() and (table = 'test_replicated_vector' OR table = 'test_replicated_vector2');
SELECT '--- After lightweight, select from test_replicated_vector2 limit 10';
SELECT id, distance(vector, [0.01,0.02,0.03,0.04,0.05,0.06,0.07,0.08,0.09,0.1,0.11,0.12,0.13,0.14,0.15,0.16,0.17,0.18,0.19,0.2,0.21,0.22,0.23,0.24,0.25,0.26,0.27,0.28,0.29,0.3,0.31,0.32 ]) AS d FROM test_replicated_vector2 ORDER BY d LIMIT 10;

SELECT '--- After lightweight, select from test_replicated_vector id>5 limit 10';
SELECT id, distance(vector, [0.01,0.02,0.03,0.04,0.05,0.06,0.07,0.08,0.09,0.1,0.11,0.12,0.13,0.14,0.15,0.16,0.17,0.18,0.19,0.2,0.21,0.22,0.23,0.24,0.25,0.26,0.27,0.28,0.29,0.3,0.31,0.32 ]) as d FROM test_replicated_vector prewhere id > 5 order by d limit 10;


SELECT '--- Decoupled part when source parts contain lightweight delete';
optimize table test_replicated_vector final;
SYSTEM WAIT BUILDING VECTOR INDICES test_replicated_vector;
select table, name, type, total_parts, status from system.vector_indices where database = currentDatabase() and (table = 'test_replicated_vector' OR table = 'test_replicated_vector2');
SELECT '--- After optimize, select from test_replicated_vector limit 10';
SELECT id, distance(vector, [0.01,0.02,0.03,0.04,0.05,0.06,0.07,0.08,0.09,0.1,0.11,0.12,0.13,0.14,0.15,0.16,0.17,0.18,0.19,0.2,0.21,0.22,0.23,0.24,0.25,0.26,0.27,0.28,0.29,0.3,0.31,0.32 ]) AS d FROM test_replicated_vector ORDER BY d LIMIT 10;
SELECT '--- After optimize, select from test_replicated_vector2 id>5 limit 10';
SELECT id, distance(vector, [0.01,0.02,0.03,0.04,0.05,0.06,0.07,0.08,0.09,0.1,0.11,0.12,0.13,0.14,0.15,0.16,0.17,0.18,0.19,0.2,0.21,0.22,0.23,0.24,0.25,0.26,0.27,0.28,0.29,0.3,0.31,0.32 ]) as d FROM test_replicated_vector2 prewhere id > 5 order by d limit 10;

SELECT '--- Lightweight delete on decoupled part';
delete from test_replicated_vector where id = 3;
delete from test_replicated_vector where id = 7;

SYSTEM WAIT BUILDING VECTOR INDICES test_replicated_vector;
SYSTEM WAIT BUILDING VECTOR INDICES test_replicated_vector2;
select table, name, type, total_parts, status from system.vector_indices where database = currentDatabase() and (table = 'test_replicated_vector' OR table = 'test_replicated_vector2');
SELECT '--- After lightweight, select from test_replicated_vector2 limit 10';
SELECT id, distance(vector, [0.01,0.02,0.03,0.04,0.05,0.06,0.07,0.08,0.09,0.1,0.11,0.12,0.13,0.14,0.15,0.16,0.17,0.18,0.19,0.2,0.21,0.22,0.23,0.24,0.25,0.26,0.27,0.28,0.29,0.3,0.31,0.32 ]) AS d FROM test_replicated_vector2 ORDER BY d LIMIT 10;
SELECT '--- After lightweight, select from test_replicated_vector id>5 limit 10';
SELECT id, distance(vector, [0.01,0.02,0.03,0.04,0.05,0.06,0.07,0.08,0.09,0.1,0.11,0.12,0.13,0.14,0.15,0.16,0.17,0.18,0.19,0.2,0.21,0.22,0.23,0.24,0.25,0.26,0.27,0.28,0.29,0.3,0.31,0.32 ]) as d FROM test_replicated_vector prewhere id > 5 order by d limit 10;

DROP TABLE IF EXISTS test_replicated_vector SYNC;
DROP TABLE IF EXISTS test_replicated_vector2 SYNC;

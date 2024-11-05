-- Tags: no-parallel

SELECT '-- Test LWD with multiple vector indices';
DROP TABLE IF EXISTS test_multi_lwd;
CREATE TABLE test_multi_lwd (`id` UInt32, `v1` Array(Float32), `v2` Array(Float32),
CONSTRAINT v1_len CHECK length(v1)=3, CONSTRAINT v2_len CHECK length(v2)=3) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_to_build_vector_index=10000, enable_rebuild_for_decouple=false;

INSERT INTO test_multi_lwd SELECT number, [number, number, number], [number+100, number+100, number+100] FROM numbers(5500);

SELECT '-- Build multiple vector indices on MPart with LWD';
DELETE FROM test_multi_lwd WHERE id = 3;

ALTER TABLE test_multi_lwd ADD VECTOR INDEX v1 v1 TYPE MSTG;
ALTER TABLE test_multi_lwd ADD VECTOR INDEX v2 v2 TYPE MSTG;

SYSTEM WAIT BUILDING VECTOR INDICES test_multi_lwd;

SELECT '-- Original VPart, Select on v1';
SELECT id, v1, distance(v1, [0.1, 0.1, 0.1]) as d FROM test_multi_lwd order by d limit 10;
SELECT '-- Original VPart, Select on v2';
SELECT id, v2, distance(v2, [100.1, 100.1, 100.1]) as d FROM test_multi_lwd order by d limit 10;

SELECT '-- LWD on VPart with multiple vector indices';
DELETE FROM test_multi_lwd WHERE id = 5;

SELECT sleep(1);

SELECT '-- After LWD on VPart, build status for multiple vector indices should be Built';
SELECT name, type, expr, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_multi_lwd';

SELECT '-- After LWD on VPart, select on v1';
SELECT id, v1, distance(v1, [0.1, 0.1, 0.1]) as d FROM test_multi_lwd order by d limit 10;
SELECT '-- After LWD on VPart, select on v2';
SELECT id, v2, distance(v2, [100.1, 100.1, 100.1]) as d FROM test_multi_lwd order by d limit 10;

SELECT '-- Decouple part on Vpart with multiple vector indices';
INSERT INTO test_multi_lwd SELECT number, [number, number, number], [number+100, number+100, number+100] FROM numbers(5500,5500);

SYSTEM WAIT BUILDING VECTOR INDICES test_multi_lwd;

SELECT '-- Before decouple part, select on v1';
SELECT id, v1, distance(v1, [0.1, 0.1, 0.1]) as d FROM test_multi_lwd order by d limit 10;
SELECT '-- Before decouple part, select on v2';
SELECT id, v2, distance(v2, [100.1, 100.1, 100.1]) as d FROM test_multi_lwd order by d limit 10;

-- Vpart with LWD + VPart -> DPart
OPTIMIZE TABLE test_multi_lwd FINAL;

SELECT '-- After decouple part, select on v1';
SELECT id, v1, distance(v1, [0.1, 0.1, 0.1]) as d FROM test_multi_lwd order by d limit 10;
SELECT '-- After decouple part, select on v2';
SELECT id, v2, distance(v2, [100.1, 100.1, 100.1]) as d FROM test_multi_lwd order by d limit 10;

SELECT '-- LWD on DPart with multiple vector indices';
DELETE FROM test_multi_lwd WHERE id = 8;

SELECT sleep(1);

SELECT '-- After LWD on DPart, select on v1';
SELECT id, v1, distance(v1, [0.1, 0.1, 0.1]) as d FROM test_multi_lwd order by d limit 10;
SELECT '-- After LWD on DPart, select on v2';
SELECT id, v2, distance(v2, [100.1, 100.1, 100.1]) as d FROM test_multi_lwd order by d limit 10;

DROP TABLE IF EXISTS test_multi_lwd;

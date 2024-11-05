-- Tags: no-parallel

SELECT '-- Test LWD with multiple vector indices';
DROP TABLE IF EXISTS test_multi_replica_lwd;
CREATE TABLE test_multi_replica_lwd (`id` UInt32, `v1` Array(Float32), `v2` Array(Float32),
CONSTRAINT v1_len CHECK length(v1)=3, CONSTRAINT v2_len CHECK length(v2)=3)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_multi_replica_lwd', 'r1') ORDER BY id
SETTINGS min_bytes_to_build_vector_index=10000, enable_rebuild_for_decouple=false;

INSERT INTO test_multi_replica_lwd SELECT number, [number, number, number], [number+100, number+100, number+100] FROM numbers(5500);

SELECT '-- Build multiple vector indices on MPart with LWD';
set mutations_sync=1;
DELETE FROM test_multi_replica_lwd WHERE id = 3;

ALTER TABLE test_multi_replica_lwd ADD VECTOR INDEX v1 v1 TYPE MSTG;
ALTER TABLE test_multi_replica_lwd ADD VECTOR INDEX v2 v2 TYPE MSTG;

SYSTEM WAIT BUILDING VECTOR INDICES test_multi_replica_lwd;

SELECT '-- Original VPart, Select on v1';
SELECT id, v1, distance(v1, [0.1, 0.1, 0.1]) as d FROM test_multi_replica_lwd order by d limit 10;
SELECT '-- Original VPart, select on v2';
SELECT id, v2, distance(v2, [100.1, 100.1, 100.1]) as d FROM test_multi_replica_lwd order by d limit 10;

SELECT '-- LWD on VPart with multiple vector indices';
DELETE FROM test_multi_replica_lwd WHERE id = 5;

SYSTEM WAIT BUILDING VECTOR INDICES test_multi_replica_lwd;
SELECT '-- After LWD on VPart, select on v1';
SELECT id, v1, distance(v1, [0.1, 0.1, 0.1]) as d FROM test_multi_replica_lwd order by d limit 10;
SELECT '-- After LWD on VPart, select on v2';
SELECT id, v2, distance(v2, [100.1, 100.1, 100.1]) as d FROM test_multi_replica_lwd order by d limit 10;

SELECT '-- Decouple part on Vpart with multiple vector indices';
INSERT INTO test_multi_replica_lwd SELECT number, [number, number, number], [number+100, number+100, number+100] FROM numbers(5500,5500);

SYSTEM WAIT BUILDING VECTOR INDICES test_multi_replica_lwd;
SELECT '-- Before decouple part, select on v1';
SELECT id, v1, distance(v1, [0.1, 0.1, 0.1]) as d FROM test_multi_replica_lwd order by d limit 10;
SELECT '-- Before decouple part, select on v2';
SELECT id, v2, distance(v2, [100.1, 100.1, 100.1]) as d FROM test_multi_replica_lwd order by d limit 10;

-- Vpart with LWD + VPart -> DPart
OPTIMIZE TABLE test_multi_replica_lwd FINAL;

SELECT '-- After decouple part, select on v1';
SELECT id, v1, distance(v1, [0.1, 0.1, 0.1]) as d FROM test_multi_replica_lwd order by d limit 10;
SELECT '-- After decouple part, select on v2';
SELECT id, v2, distance(v2, [100.1, 100.1, 100.1]) as d FROM test_multi_replica_lwd order by d limit 10;

SELECT '-- LWD on DPart with multiple vector indices';
DELETE FROM test_multi_replica_lwd WHERE id = 8;

SYSTEM WAIT BUILDING VECTOR INDICES test_multi_replica_lwd;
SELECT '-- After LWD on DPart, select on v1';
SELECT id, v1, distance(v1, [0.1, 0.1, 0.1]) as d FROM test_multi_replica_lwd order by d limit 10;
SELECT '-- After LWD on DPart, select on v2';
SELECT id, v2, distance(v2, [100.1, 100.1, 100.1]) as d FROM test_multi_replica_lwd order by d limit 10;

DROP TABLE IF EXISTS test_multi_replica_lwd;

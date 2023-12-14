-- Tags: no-parallel

SELECT '-- Test table with multiple vector indices';

DROP TABLE IF EXISTS test_multi_replica;
CREATE TABLE test_multi_replica (`id` UInt32, `v1` Array(Float32), `v2` Array(Float32),
VECTOR INDEX v1 v1 TYPE MSTG, VECTOR INDEX v2 v2 TYPE MSTG,
CONSTRAINT v1_len CHECK length(v1)=3, CONSTRAINT v2_len CHECK length(v2)=3)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_multi_replica', 'r1') ORDER BY id
SETTINGS min_bytes_to_build_vector_index=10000;

INSERT INTO test_multi_replica SELECT number, [number, number, number], [number+100, number+100, number+100] FROM numbers(5500);

SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95) ) FROM (select status from system.vector_indices where table = 'test_multi_replica' and name = 'v2' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_multi_replica' and name = 'v2' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_multi_replica' and name = 'v2' and database = currentDatabase());

SELECT '-- Check build status for multiple vector indices';
SELECT name, type, expr, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_multi_replica';

SELECT '-- Drop a vector index v1';
ALTER TABLE test_multi_replica DROP VECTOR INDEX v1;
SELECT name, type, expr, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_multi_replica';

SELECT '-- Check system table vector_index_segments';
SELECT part, name, status FROM system.vector_index_segments WHERE database = currentDatabase() and table = 'test_multi_replica' order by name;

ALTER TABLE test_multi_replica ADD VECTOR INDEX v1 v1 TYPE MSTG;

SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95) ) FROM (select status from system.vector_indices where table = 'test_multi_replica' and name = 'v1' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_multi_replica' and name = 'v1' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_multi_replica' and name = 'v1' and database = currentDatabase());

SELECT '-- After add a second index on VPart, check build status for multiple vector indices';
SELECT name, type, expr, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_multi_replica';

SELECT '-- Insert a new part for the test of VParts -> DPart';
INSERT INTO test_multi_replica SELECT number, [number, number, number], [number+100, number+100, number+100] FROM numbers(5500,5500);

SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95) ) FROM (select status from system.vector_indices where table = 'test_multi_replica' and name = 'v1' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_multi_replica' and name = 'v1' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_multi_replica' and name = 'v1' and database = currentDatabase());

SELECT 'Before decouple, two VParts with multiple vector indices';
SELECT name, type, expr, total_parts, parts_with_vector_index, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_multi_replica';

SYSTEM STOP BUILD VECTOR INDICES test_multi_replica;
OPTIMIZE TABLE test_multi_replica FINAL;

SELECT '-- After decouple, check system table vector_index_segments';
SELECT name, part, owner_part, owner_part_id, status FROM system.vector_index_segments WHERE database = currentDatabase() and table = 'test_multi_replica' order by name, part, owner_part_id;

SELECT '-- After decouple, check system table vector_indices';
SELECT name, type, expr, total_parts, parts_with_vector_index, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_multi_replica';

SYSTEM START BUILD VECTOR INDICES test_multi_replica;

SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95) ) FROM (select status from system.vector_indices where table = 'test_multi_replica' and name = 'v1' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_multi_replica' and name = 'v1' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_multi_replica' and name = 'v1' and database = currentDatabase());

SELECT '-- DPart->VPart, check system table vector_indices';
SELECT name, type, expr, total_parts, parts_with_vector_index, status FROM system.vector_indices WHERE database = currentDatabase() and table = 'test_multi_replica';

DROP TABLE IF EXISTS test_multi_replica;

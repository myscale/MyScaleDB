-- Tags: no-parallel

DROP TABLE IF EXISTS test_refactor_lwd;
CREATE TABLE test_refactor_lwd(id int, b int) engine MergeTree order by id SETTINGS index_granularity=18, min_bytes_for_wide_part=0;
INSERT INTO test_refactor_lwd SELECT number, number FROM numbers(188);

-- default lightweight_deletes_sync = 2
set allow_experimental_optimized_lwd = true;
SELECT 'Lightweight delete on part w/o mask column';
DELETE FROM test_refactor_lwd where id = 2;
SELECT 'Select after delete id=2';
SELECT id, b FROM test_refactor_lwd order by id limit 5;
SELECT 'Lightweight delete on part with mask column';
DELETE FROM test_refactor_lwd where id = 5;
SELECT 'Select after delete id=2 and 5';
SELECT id, b FROM test_refactor_lwd order by id limit 5;

set allow_experimental_optimized_lwd = false;
SELECT 'Back to old Lightweight delete';
DELETE FROM test_refactor_lwd where id = 4;
SELECT 'Select after delete id=2, 4 and 5';
SELECT id, b FROM test_refactor_lwd order by id limit 5;

SELECT mutation_id, command FROM system.mutations WHERE database = currentDatabase() and table='test_refactor_lwd';

DROP TABLE test_refactor_lwd;
set allow_experimental_optimized_lwd = true;

DROP TABLE IF EXISTS test_refactor_lwd_vector;
CREATE TABLE test_refactor_lwd_vector(id int, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3)
engine MergeTree order by id SETTINGS index_granularity=18;
ALTER TABLE test_refactor_lwd_vector ADD VECTOR INDEX v1 vector TYPE FLAT;

INSERT INTO test_refactor_lwd_vector SELECT number, [number, number, number] FROM numbers(100);

SYSTEM WAIT BUILDING VECTOR INDICES test_refactor_lwd_vector;

SELECT '--- Original topK result';
SELECT id, distance(vector, [1.0, 1, 1]) as d FROM test_refactor_lwd_vector order by d limit 5;
SELECT '--- Lightweight delete on part with vector index';
delete from test_refactor_lwd_vector where id = 2;
SYSTEM WAIT BUILDING VECTOR INDICES test_refactor_lwd_vector;

SELECT '--- After lightweight delete, new topK result';
SELECT id, distance(vector, [1.0, 1, 1]) as d FROM test_refactor_lwd_vector order by d limit 5;

KILL MUTATION WHERE table = 'test_refactor_lwd_vector' and database = currentDatabase() format Null;

DROP TABLE test_refactor_lwd_vector;

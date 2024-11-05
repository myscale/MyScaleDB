DROP TABLE IF EXISTS test_decouple_vector;
CREATE TABLE test_decouple_vector(id UInt32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 768) engine MergeTree primary key id SETTINGS enable_decouple_vector_index=true;
INSERT INTO test_decouple_vector SELECT number as id, arrayMap(x -> (rand() % 10000000) / 10000000.0 * (if(rand() % 2 = 0, 1, -1)), range(768)) as vector FROM numbers(2100);
INSERT INTO test_decouple_vector SELECT number as id, arrayMap(x -> (rand() % 10000000) / 10000000.0 * (if(rand() % 2 = 0, 1, -1)), range(768)) as vector FROM numbers(2100, 1001);

ALTER TABLE test_decouple_vector ADD VECTOR INDEX v1 vector TYPE MSTG;

SYSTEM WAIT BUILDING VECTOR INDICES test_decouple_vector;

SELECT 'Test decouple data part enabled';

SELECT table, name, total_parts, status FROM system.vector_indices WHERE database=currentDatabase() and table='test_decouple_vector';

OPTIMIZE TABLE test_decouple_vector FINAL;

SELECT table, part, owner_part, owner_part_id FROM system.vector_index_segments WHERE database=currentDatabase() and table='test_decouple_vector' order by owner_part_id;

DROP TABLE test_decouple_vector;

DROP TABLE IF EXISTS test_disable_decouple_vector;
CREATE TABLE test_disable_decouple_vector(id UInt32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 768) engine MergeTree primary key id SETTINGS enable_decouple_vector_index=false;
INSERT INTO test_disable_decouple_vector SELECT number as id, arrayMap(x -> (rand() % 10000000) / 10000000.0 * (if(rand() % 2 = 0, 1, -1)), range(768)) as vector FROM numbers(2100);
INSERT INTO test_disable_decouple_vector SELECT number as id, arrayMap(x -> (rand() % 10000000) / 10000000.0 * (if(rand() % 2 = 0, 1, -1)), range(768)) as vector FROM numbers(2100, 1001);

ALTER TABLE test_disable_decouple_vector ADD VECTOR INDEX v1 vector TYPE MSTG;

SYSTEM WAIT BUILDING VECTOR INDICES test_disable_decouple_vector;
SELECT 'Test decouple data part disabled';

SELECT table, name, total_parts, status FROM system.vector_indices WHERE database=currentDatabase() and table='test_disable_decouple_vector';

OPTIMIZE TABLE test_disable_decouple_vector FINAL;

SELECT table, part, owner_part, owner_part_id FROM system.vector_index_segments WHERE database=currentDatabase() and table='test_disable_decouple_vector';

SYSTEM WAIT BUILDING VECTOR INDICES test_disable_decouple_vector;

SELECT table, part, owner_part, owner_part_id FROM system.vector_index_segments WHERE database=currentDatabase() and table='test_disable_decouple_vector';

DROP TABLE test_disable_decouple_vector;

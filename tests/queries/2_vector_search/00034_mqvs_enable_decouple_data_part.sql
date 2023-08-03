DROP TABLE IF EXISTS test_decouple_vector;
CREATE TABLE test_decouple_vector(id UInt32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) engine MergeTree primary key id SETTINGS enable_decouple_vector_index=true;
INSERT INTO test_decouple_vector SELECT number, [number, number, number] FROM numbers(2100);
INSERT INTO test_decouple_vector SELECT number, [number, number, number] FROM numbers(2100,1001);

ALTER TABLE test_decouple_vector ADD VECTOR INDEX v1 vector TYPE MSTG;

SELECT sleep(3);
SELECT 'Test decouple data part enabled';

SELECT table, name, total_parts, status FROM system.vector_indices WHERE database=currentDatabase() and table='test_decouple_vector';

OPTIMIZE TABLE test_decouple_vector FINAL;

SELECT table, part, owner_part, owner_part_id FROM system.vector_index_segments WHERE database=currentDatabase() and table='test_decouple_vector' order by owner_part_id;

DROP TABLE test_decouple_vector;

DROP TABLE IF EXISTS test_disable_decouple_vector;
CREATE TABLE test_disable_decouple_vector(id UInt32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) engine MergeTree primary key id SETTINGS enable_decouple_vector_index=false;
INSERT INTO test_disable_decouple_vector SELECT number, [number, number, number] FROM numbers(2100);
INSERT INTO test_disable_decouple_vector SELECT number, [number, number, number] FROM numbers(2100,1001);

ALTER TABLE test_disable_decouple_vector ADD VECTOR INDEX v1 vector TYPE MSTG;

SELECT sleep(3);
SELECT 'Test decouple data part disabled';

SELECT table, name, total_parts, status FROM system.vector_indices WHERE database=currentDatabase() and table='test_disable_decouple_vector';

OPTIMIZE TABLE test_disable_decouple_vector FINAL;

SELECT table, part, owner_part, owner_part_id FROM system.vector_index_segments WHERE database=currentDatabase() and table='test_disable_decouple_vector';

SELECT sleep(3);

SELECT table, part, owner_part, owner_part_id FROM system.vector_index_segments WHERE database=currentDatabase() and table='test_disable_decouple_vector';

DROP TABLE test_disable_decouple_vector;

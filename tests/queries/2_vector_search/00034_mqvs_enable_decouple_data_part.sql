DROP TABLE IF EXISTS test_decouple_vector;
CREATE TABLE test_decouple_vector(id UInt32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 768) engine MergeTree primary key id SETTINGS enable_decouple_vector_index=true;
INSERT INTO test_decouple_vector SELECT number as id, arrayMap(x -> (rand() % 10000000) / 10000000.0 * (if(rand() % 2 = 0, 1, -1)), range(768)) as vector FROM numbers(2100);
INSERT INTO test_decouple_vector SELECT number as id, arrayMap(x -> (rand() % 10000000) / 10000000.0 * (if(rand() % 2 = 0, 1, -1)), range(768)) as vector FROM numbers(2100, 1001);

ALTER TABLE test_decouple_vector ADD VECTOR INDEX v1 vector TYPE MSTG;
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95) ) FROM (select status from system.vector_indices where table = 'test_decouple_vector' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_decouple_vector' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_decouple_vector' and database = currentDatabase());

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

SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95) ) FROM (select status from system.vector_indices where table = 'test_disable_decouple_vector' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_disable_decouple_vector' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_disable_decouple_vector' and database = currentDatabase());
SELECT 'Test decouple data part disabled';

SELECT table, name, total_parts, status FROM system.vector_indices WHERE database=currentDatabase() and table='test_disable_decouple_vector';

OPTIMIZE TABLE test_disable_decouple_vector FINAL;

SELECT table, part, owner_part, owner_part_id FROM system.vector_index_segments WHERE database=currentDatabase() and table='test_disable_decouple_vector';

SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95) ) FROM (select status from system.vector_indices where table = 'test_disable_decouple_vector' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_disable_decouple_vector' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_disable_decouple_vector' and database = currentDatabase());

SELECT table, part, owner_part, owner_part_id FROM system.vector_index_segments WHERE database=currentDatabase() and table='test_disable_decouple_vector';

DROP TABLE test_disable_decouple_vector;

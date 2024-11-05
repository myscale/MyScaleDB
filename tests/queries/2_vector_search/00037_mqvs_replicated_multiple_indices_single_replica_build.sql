-- Tags: no-parallel

DROP TABLE IF EXISTS test_rep_multi SYNC;
DROP TABLE IF EXISTS test_rep_multi_2 SYNC;
CREATE TABLE test_rep_multi(id Float32, vector Array(Float32), v2 Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 32, CONSTRAINT v2_len CHECK length(v2) = 32) 
engine=ReplicatedMergeTree('/clickhouse/tables/{database}/test_rep_multi', 'r1') primary key id
SETTINGS min_bytes_to_build_vector_index=10000, build_vector_index_on_random_single_replica=1;

CREATE TABLE test_rep_multi_2(id Float32, vector Array(Float32), v2 Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 32, CONSTRAINT v2_len CHECK length(v2) = 32) 
engine=ReplicatedMergeTree('/clickhouse/tables/{database}/test_rep_multi', 'r2') primary key id
SETTINGS min_bytes_to_build_vector_index=10000, build_vector_index_on_random_single_replica=1;

ALTER TABLE test_rep_multi ADD VECTOR INDEX v1 vector TYPE MSTG;

INSERT INTO test_rep_multi SELECT number, arrayMap(x -> 0.0008 * (number * 32 + x + 1) * (if(x % 2 = 0, -1, 1)), range(32)), arrayMap(x -> 0.0008 * (number * 32 + x + 1) * (if(x % 2 = 0, -1, 1)), range(32)) FROM numbers(2100);
SYSTEM WAIT BUILDING VECTOR INDICES test_rep_multi;
SYSTEM WAIT BUILDING VECTOR INDICES test_rep_multi_2;

SELECT table, name, type, status FROM system.vector_indices WHERE table like 'test_rep_multi%' AND database=currentDatabase() order by table,name;

SELECT 'Add a second vector index';
ALTER TABLE test_rep_multi ADD VECTOR INDEX v2 v2 TYPE MSTG;
SYSTEM WAIT BUILDING VECTOR INDICES test_rep_multi;
SYSTEM WAIT BUILDING VECTOR INDICES test_rep_multi_2;

SELECT table, name, type, status FROM system.vector_indices WHERE table like 'test_rep_multi%' AND database=currentDatabase() order by table,name;

SELECT 'Test drop one of two replicas, vector index should build sucessfully';
DROP TABLE test_rep_multi_2 SYNC;

INSERT INTO test_rep_multi SELECT number, arrayMap(x -> 0.0008 * (number * 32 + x + 1) * (if(x % 2 = 0, -1, 1)), range(32)), arrayMap(x -> 0.0008 * (number * 32 + x + 1) * (if(x % 2 = 0, -1, 1)), range(32)) FROM numbers(2100, 2000);
SYSTEM WAIT BUILDING VECTOR INDICES test_rep_multi;

SELECT table, name, type, total_parts, parts_with_vector_index, status FROM system.vector_indices WHERE table like 'test_rep_multi%' AND database=currentDatabase() order by table,name;

DROP TABLE test_rep_multi SYNC;

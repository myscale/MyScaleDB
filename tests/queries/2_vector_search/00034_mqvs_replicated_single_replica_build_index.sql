-- Tags: no-parallel

DROP TABLE IF EXISTS t_rep_vector SYNC;
DROP TABLE IF EXISTS t_rep_vector_2 SYNC;

CREATE TABLE t_rep_vector(id Float32, vector Array(Float32), CONSTRAINT check_length CHECK length(vector) = 3) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/t_rep_vector','1') order by id SETTINGS index_granularity=128;
CREATE TABLE t_rep_vector_2(id Float32, vector Array(Float32), CONSTRAINT check_length CHECK length(vector) = 3) engine=ReplicatedMergeTree('/clickhouse/tables/{database}/t_rep_vector','2') order by id SETTINGS index_granularity=128;

CREATE VECTOR INDEX v1 ON t_rep_vector vector TYPE FLAT;

INSERT INTO t_rep_vector SELECT number, [number, number, number] FROM numbers(10000);
SYSTEM WAIT BUILDING VECTOR INDICES t_rep_vector;

SELECT table, name, status FROM system.vector_indices WHERE table like 't_rep_vector%' AND database=currentDatabase();

SELECT 'Test drop one of two replicas, vector index should build sucessfully';
DROP TABLE t_rep_vector_2 sync;

INSERT INTO t_rep_vector SELECT number, [number, number, number] FROM numbers(10000,5000);
SYSTEM WAIT BUILDING VECTOR INDICES t_rep_vector;

SELECT table, name, status FROM system.vector_indices WHERE table='t_rep_vector' AND database=currentDatabase();

DROP TABLE t_rep_vector sync;

select name from system.merge_tree_settings where name = 'build_vector_index_on_random_single_replica';

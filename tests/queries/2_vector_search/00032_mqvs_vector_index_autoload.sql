-- Tags: zookeeper

DROP TABLE IF EXISTS test_vector_index_autoload SYNC;
CREATE TABLE test_vector_index_autoload
(
    id    UInt32,
    vector  Array(Float32),
    CONSTRAINT check_length CHECK length(vector) = 3
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/'||currentDatabase()||'/mqvs_00032/test_vector_index_autoload/s1', 'r1')
ORDER BY id
SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1, max_rows_for_slow_mode_single_vector_index_build=10;

SELECT '--- init';

INSERT INTO test_vector_index_autoload SELECT number, [number,number,number] FROM numbers(100);

SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/'||currentDatabase()||'/mqvs_00032/test_vector_index_autoload/s1/replicas/r1' AND name = 'vidx_info';

ALTER TABLE test_vector_index_autoload ADD VECTOR INDEX vec_ind vector TYPE HNSWFLAT;

SYSTEM WAIT BUILDING VECTOR INDICES test_vector_index_autoload;

SELECT part, owner_part, owner_part_id, name, status FROM system.vector_index_segments where database = currentDatabase() AND table = 'test_vector_index_autoload';

SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/'||currentDatabase()||'/mqvs_00032/test_vector_index_autoload/s1/replicas/r1' AND name = 'vidx_info';

SELECT '--- drop vector index';

ALTER TABLE test_vector_index_autoload DROP VECTOR INDEX vec_ind;

SELECT sleep(2);

SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/'||currentDatabase()||'/mqvs_00032/test_vector_index_autoload/s1/replicas/r1' AND name = 'vidx_info';

SELECT '--- add vector index';

ALTER TABLE test_vector_index_autoload ADD VECTOR INDEX vec_ind vector TYPE HNSWFLAT;

SYSTEM WAIT BUILDING VECTOR INDICES test_vector_index_autoload;
SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/'||currentDatabase()||'/mqvs_00032/test_vector_index_autoload/s1/replicas/r1' AND name = 'vidx_info';

SELECT '--- insert again';

INSERT INTO test_vector_index_autoload SELECT number+100, [number+100,number+100,number+100] FROM numbers(100);

SYSTEM WAIT BUILDING VECTOR INDICES test_vector_index_autoload;

SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/'||currentDatabase()||'/mqvs_00032/test_vector_index_autoload/s1/replicas/r1' AND name = 'vidx_info';

SELECT '--- merge';

OPTIMIZE TABLE test_vector_index_autoload FINAL;

SYSTEM WAIT BUILDING VECTOR INDICES test_vector_index_autoload;

SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/'||currentDatabase()||'/mqvs_00032/test_vector_index_autoload/s1/replicas/r1' AND name = 'vidx_info';

SELECT '--- drop table';

DROP TABLE test_vector_index_autoload SYNC;

SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/'||currentDatabase()||'/mqvs_00032/test_vector_index_autoload/s1/replicas/r1' AND name = 'vidx_info';

CREATE TABLE test_vector_index_autoload
(
    id    UInt32,
    vector  Array(Float32),
    CONSTRAINT check_length CHECK length(vector) = 3
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/'||currentDatabase()||'/mqvs_00032/test_vector_index_autoload/s1', 'r1')
ORDER BY id
SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1, enable_rebuild_for_decouple=false, max_rows_for_slow_mode_single_vector_index_build=10;

SELECT '--- init (enable_rebuild_for_decouple)';

INSERT INTO test_vector_index_autoload SELECT number, [number,number,number] FROM numbers(100);

ALTER TABLE test_vector_index_autoload ADD VECTOR INDEX vec_ind vector TYPE HNSWFLAT;

SYSTEM WAIT BUILDING VECTOR INDICES test_vector_index_autoload;

SELECT part, owner_part, owner_part_id, name, status FROM system.vector_index_segments where database = currentDatabase() AND table = 'test_vector_index_autoload';

SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/'||currentDatabase()||'/mqvs_00032/test_vector_index_autoload/s1/replicas/r1' AND name = 'vidx_info';

SELECT '--- lightweight delete';

DELETE FROM test_vector_index_autoload WHERE id = 3;

SELECT sleep(1);

SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/'||currentDatabase()||'/mqvs_00032/test_vector_index_autoload/s1/replicas/r1' AND name = 'vidx_info';

SELECT '--- insert again';

INSERT INTO test_vector_index_autoload SELECT number+100, [number+100,number+100,number+100] FROM numbers(100);

SYSTEM WAIT BUILDING VECTOR INDICES test_vector_index_autoload;

SELECT part, owner_part, owner_part_id, name, status FROM system.vector_index_segments where database = currentDatabase() AND table = 'test_vector_index_autoload' ORDER BY part, owner_part;

SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/'||currentDatabase()||'/mqvs_00032/test_vector_index_autoload/s1/replicas/r1' AND name = 'vidx_info';

SELECT '--- merge';

OPTIMIZE TABLE test_vector_index_autoload FINAL;

SELECT part, owner_part, owner_part_id, name, status FROM system.vector_index_segments where database = currentDatabase() AND table = 'test_vector_index_autoload' ORDER BY part, owner_part;

SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/'||currentDatabase()||'/mqvs_00032/test_vector_index_autoload/s1/replicas/r1' AND name = 'vidx_info';

DROP TABLE test_vector_index_autoload SYNC;
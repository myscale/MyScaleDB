-- Tags: no-parallel

DROP TABLE IF EXISTS test_vector_segments SYNC;
CREATE TABLE test_vector_segments
(
    id    UInt32,
    vector  Array(Float32),
    CONSTRAINT check_length CHECK length(vector) = 3
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1, disable_rebuild_for_decouple=true, max_rows_for_slow_mode_single_vector_index_build = 10;

INSERT INTO test_vector_segments SELECT number, [number,number,number] FROM numbers(50000);

ALTER TABLE test_vector_segments ADD VECTOR INDEX vec_ind vector TYPE HNSWFLAT;

-- waiting index create finished, 2s/4s/8s/16s/16s/16s
SELECT if(status='Built', sleep(0), sleep(2) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());

SELECT part, owner_part, owner_part_id, name, status FROM system.vector_index_segments where database = currentDatabase() AND table = 'test_vector_segments';

SELECT '--- detach';

DETACH TABLE test_vector_segments SYNC;

SELECT part, owner_part, owner_part_id, name, status FROM system.vector_index_segments where database = currentDatabase() AND table = 'test_vector_segments';

SELECT '--- attach';

ATTACH TABLE test_vector_segments;

SELECT part, owner_part, owner_part_id, name, status FROM system.vector_index_segments where database = currentDatabase() AND table = 'test_vector_segments';

SELECT '--- query';

SELECT id, distance(vector, [1.2, 2.3, 3.4]) AS dist FROM test_vector_segments order by dist limit 10;

SELECT sleep(2);

SELECT part, owner_part, owner_part_id, name, status FROM system.vector_index_segments where database = currentDatabase() AND table = 'test_vector_segments';

SELECT '--- drop vector index';

ALTER TABLE test_vector_segments DROP VECTOR INDEX vec_ind;

SELECT part, owner_part, owner_part_id, name, status FROM system.vector_index_segments where database = currentDatabase() AND table = 'test_vector_segments';

SELECT '--- add vector index';

ALTER TABLE test_vector_segments ADD VECTOR INDEX vec_ind vector TYPE HNSWFLAT;

-- waiting index create finished, 2s/4s/8s/16s/16s/16s
SELECT if(status='Built', sleep(0), sleep(2) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());


SELECT part, owner_part, owner_part_id, name, status FROM system.vector_index_segments where database = currentDatabase() AND table = 'test_vector_segments';

SELECT '--- truncate';

TRUNCATE TABLE test_vector_segments SYNC;

SELECT part, owner_part, owner_part_id, name, status FROM system.vector_index_segments where database = currentDatabase() AND table = 'test_vector_segments';

SELECT sleep(2);

SELECT part, owner_part, owner_part_id, name, status FROM system.vector_index_segments where database = currentDatabase() AND table = 'test_vector_segments';

SELECT '--- insert';

INSERT INTO test_vector_segments SELECT number, [number,number,number] FROM numbers(50000);

-- waiting index create finished, 2s/4s/8s/16s/16s/16s
SELECT if(status='Built', sleep(0), sleep(2) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());

SELECT part, owner_part, owner_part_id, name, status FROM system.vector_index_segments where database = currentDatabase() AND table = 'test_vector_segments';

SELECT '--- lightweight delete';

DELETE FROM test_vector_segments WHERE id = 3;

SELECT part, owner_part, owner_part_id, name, status FROM system.vector_index_segments where database = currentDatabase() AND table = 'test_vector_segments';

SELECT '--- insert';

INSERT INTO test_vector_segments SELECT number+1000, [number+1000,number+1000,number+1000] FROM numbers(20000);

-- waiting index create finished, 2s/4s/8s/16s/16s/16s
SELECT if(status='Built', sleep(0), sleep(2) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());

SELECT part, owner_part, owner_part_id, name, status FROM system.vector_index_segments where database = currentDatabase() AND table = 'test_vector_segments' order by owner_part_id;

SELECT '--- merge';

OPTIMIZE TABLE test_vector_segments FINAL;

SELECT part, owner_part, owner_part_id, name, status FROM system.vector_index_segments where database = currentDatabase() AND table = 'test_vector_segments' order by owner_part_id;

SELECT '--- query';

SELECT id, distance(vector, [1.2, 2.3, 3.4]) AS dist FROM test_vector_segments order by dist limit 10;

SELECT sleep(2);

SELECT part, owner_part, owner_part_id, name, status FROM system.vector_index_segments where database = currentDatabase() AND table = 'test_vector_segments' order by owner_part_id;

SELECT '--- drop table';

DROP TABLE test_vector_segments SYNC;

SELECT count() FROM system.vector_index_segments where database = currentDatabase() AND table = 'test_vector_segments';

SELECT '--- test auto build after merge';

CREATE TABLE test_vector_segments
(
    id    UInt32,
    vector  Array(Float32),
    CONSTRAINT check_length CHECK length(vector) = 3
)
    ENGINE = MergeTree
        ORDER BY id
        SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1, max_rows_for_slow_mode_single_vector_index_build = 10;

SELECT '--- insert part 1';

INSERT INTO test_vector_segments SELECT number+10, [number+1.2,number+1.3,number+1.4] FROM numbers(10000);

ALTER TABLE test_vector_segments ADD VECTOR INDEX vec_ind vector TYPE HNSWFLAT;

-- waiting index create finished, 2s/4s/8s/16s/16s/16s
SELECT if(status='Built', sleep(0), sleep(2) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());

SELECT part, owner_part, owner_part_id, name, status FROM system.vector_index_segments where database = currentDatabase() AND table = 'test_vector_segments';

SELECT '--- insert part 2';

INSERT INTO test_vector_segments SELECT number+20000, [number+2.2,number+2.3,number+2.4] FROM numbers(50000);

-- waiting index create finished, 2s/4s/8s/16s/16s/16s
SELECT if(status='Built', sleep(0), sleep(2) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());

SELECT part, owner_part, owner_part_id, name, status FROM system.vector_index_segments where database = currentDatabase() AND table = 'test_vector_segments' order by owner_part_id;

SELECT '--- merge';
OPTIMIZE TABLE test_vector_segments FINAL;

SELECT if(status='Built', sleep(0), sleep(2) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());
SELECT if(status='Built', sleep(0), sleep(1.99)+sleep(1.98)+sleep(1.97)+sleep(1.96)+sleep(1.95)+sleep(1.94)+sleep(1.93)+sleep(1.92)+sleep(1.91)+sleep(1.90) ) FROM (select status from system.vector_indices where table = 'test_vector_segments' and database = currentDatabase());


SELECT owner_part_id, name, status FROM system.vector_index_segments where database = currentDatabase() AND table = 'test_vector_segments';

DROP TABLE test_vector_segments SYNC;
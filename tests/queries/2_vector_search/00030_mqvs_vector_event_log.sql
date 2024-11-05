-- Tags: no-parallel

DROP TABLE IF EXISTS test_vector_event_log SYNC;
SYSTEM FLUSH LOGS;
TRUNCATE TABLE system.vector_index_event_log SYNC;
CREATE TABLE test_vector_event_log
(
    id    UInt32,
    vector  Array(Float32),
    CONSTRAINT check_length CHECK length(vector) = 3
)
engine = MergeTree 
ORDER BY id;

INSERT INTO test_vector_event_log SELECT number, [number,number,number] FROM numbers(1000);

ALTER TABLE test_vector_event_log ADD VECTOR INDEX vec_ind vector TYPE IVFFLAT;

SYSTEM WAIT BUILDING VECTOR INDICES test_vector_event_log;

DETACH TABLE test_vector_event_log SYNC;

ATTACH TABLE test_vector_event_log;

select id, distance(vector, [1.2, 2.3, 3.4]) as dist from test_vector_event_log order by dist limit 10;

ALTER TABLE test_vector_event_log DROP VECTOR INDEX vec_ind;

ALTER TABLE test_vector_event_log ADD VECTOR INDEX vec_ind vector TYPE IVFFLAT;

SYSTEM WAIT BUILDING VECTOR INDICES test_vector_event_log;

SYSTEM FLUSH LOGS;

TRUNCATE TABLE test_vector_event_log SYNC;

INSERT INTO test_vector_event_log SELECT number, [number,number,number] FROM numbers(1000);

SYSTEM WAIT BUILDING VECTOR INDICES test_vector_event_log;

SYSTEM FLUSH LOGS;

DROP TABLE test_vector_event_log SYNC;

SELECT sleep(2);

SYSTEM FLUSH LOGS;

SELECT sleep(2);
SELECT sleep(2);

SELECT table, event_type 
FROM system.vector_index_event_log 
WHERE table = 'test_vector_event_log'
ORDER BY event_time_microseconds;




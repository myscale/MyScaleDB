-- Tags: no-parallel

DROP TABLE IF EXISTS t_create_default_index;

-- create table and create default vector indices
CREATE TABLE t_create_default_index(
    id UInt64,
    vector_1 Array(Float32),
    vector_2 Array(Float32),
    vector_3 Array(Float32),
    vector_4 Array(Float32),
    vector_5 Array(Float32),
    vector_6 Array(Float32),
    vector_7 FixedString(3),
    vector_8 FixedString(3),
    vector_9 FixedString(3),
    vector_10 FixedString(3),
    vector_11 FixedString(3),
    vector_12 FixedString(3),
    CONSTRAINT vector_len_1 CHECK length(vector_1) = 3,
    CONSTRAINT vector_len_2 CHECK length(vector_2) = 3,
    CONSTRAINT vector_len_3 CHECK length(vector_3) = 3,
    CONSTRAINT vector_len_4 CHECK length(vector_4) = 3,
    CONSTRAINT vector_len_5 CHECK length(vector_5) = 3,
    CONSTRAINT vector_len_6 CHECK length(vector_6) = 3,
    VECTOR INDEX vec_ind_1 vector_1,
    VECTOR INDEX vec_ind_2 vector_2 TYPE default('metric_type=IP'),
    VECTOR INDEX vec_ind_7 vector_7,
    VECTOR INDEX vec_ind_8 vector_8 TYPE default('metric_type=Jaccard'),
) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_create_default_index SELECT
    number,
    [number, number, number],
    [number, number, number],
    [number, number, number],
    [number, number, number],
    [number, number, number],
    [number, number, number],
    char(number, number, number),
    char(number, number, number),
    char(number, number, number),
    char(number, number, number),
    char(number, number, number),
    char(number, number, number)
FROM numbers(10);

SELECT 'Create default vector indices when creating table';
SELECT name, type, expr FROM system.vector_indices WHERE table = 't_create_default_index';


-- alter table to add default vector indices
ALTER TABLE t_create_default_index ADD VECTOR INDEX vec_ind_3 vector_3;
ALTER TABLE t_create_default_index ADD VECTOR INDEX vec_ind_4 vector_4 TYPE default;

ALTER TABLE t_create_default_index ADD VECTOR INDEX vec_ind_9 vector_9;
ALTER TABLE t_create_default_index ADD VECTOR INDEX vec_ind_10 vector_10 TYPE default;

SELECT 'Create default vector indices in ALTER TABLE ADD VECTOR INDEX query';
SELECT name, type, expr FROM system.vector_indices WHERE table = 't_create_default_index';

-- create default vector indices
CREATE VECTOR INDEX vec_ind_5 ON t_create_default_index vector_5;
CREATE VECTOR INDEX vec_ind_6 ON t_create_default_index vector_6 TYPE default;

CREATE VECTOR INDEX vec_ind_11 ON t_create_default_index vector_11;
CREATE VECTOR INDEX vec_ind_12 ON t_create_default_index vector_12 TYPE default;

SELECT 'Create default vector indices in CREATE VECTOR INDEX query';
SELECT name, type, expr FROM system.vector_indices WHERE table = 't_create_default_index';

DROP TABLE IF EXISTS t_create_default_index;

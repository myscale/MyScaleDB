import pytest
import time
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", stay_alive=True, main_configs=["configs/config_information.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_primary_key_cache_uint16(started_cluster):
    instance.query("DROP TABLE IF EXISTS test_pk_cache")
    instance.query(
        """
        CREATE TABLE test_pk_cache(id UInt16, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3)
        engine MergeTree primary key id
        SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1000, enable_primary_key_cache=true;
        INSERT INTO test_pk_cache SELECT number, [number, number, number] FROM numbers(2100);
        ALTER TABLE test_pk_cache ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
        """)

    time.sleep(2)

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Miss primary key cache")

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Hit primary key cache")


def test_primary_key_cache_uint32(started_cluster):
    instance.query("DROP TABLE IF EXISTS test_pk_cache")
    instance.query(
        """
        CREATE TABLE test_pk_cache(id UInt32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3)
        engine MergeTree primary key id
        SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1000, enable_primary_key_cache=true;
        INSERT INTO test_pk_cache SELECT number, [number, number, number] FROM numbers(2100);
        ALTER TABLE test_pk_cache ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
        """)

    time.sleep(2)

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Miss primary key cache")

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Hit primary key cache")


def test_primary_key_cache_uint64(started_cluster):
    instance.query("DROP TABLE IF EXISTS test_pk_cache")
    instance.query(
        """
        CREATE TABLE test_pk_cache(id UInt64, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3)
        engine MergeTree primary key id
        SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1000, enable_primary_key_cache=true;
        INSERT INTO test_pk_cache SELECT number, [number, number, number] FROM numbers(2100);
        ALTER TABLE test_pk_cache ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
        """)

    time.sleep(2)

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Miss primary key cache")

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Hit primary key cache")


def test_primary_key_cache_uint128(started_cluster):
    instance.query("DROP TABLE IF EXISTS test_pk_cache")
    instance.query(
        """
        CREATE TABLE test_pk_cache(id UInt128, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3)
        engine MergeTree primary key id
        SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1000, enable_primary_key_cache=true;
        INSERT INTO test_pk_cache SELECT number, [number, number, number] FROM numbers(2100);
        ALTER TABLE test_pk_cache ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
        """)

    time.sleep(2)

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Miss primary key cache")

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Hit primary key cache")


def test_primary_key_cache_uint256(started_cluster):
    instance.query("DROP TABLE IF EXISTS test_pk_cache")
    instance.query(
        """
        CREATE TABLE test_pk_cache(id UInt256, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3)
        engine MergeTree primary key id
        SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1000, enable_primary_key_cache=true;
        INSERT INTO test_pk_cache SELECT number, [number, number, number] FROM numbers(2100);
        ALTER TABLE test_pk_cache ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
        """)

    time.sleep(2)

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Miss primary key cache")

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Hit primary key cache")


def test_primary_key_cache_int16(started_cluster):
    instance.query("DROP TABLE IF EXISTS test_pk_cache")
    instance.query(
        """
        CREATE TABLE test_pk_cache(id Int16, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3)
        engine MergeTree primary key id
        SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1000, enable_primary_key_cache=true;
        INSERT INTO test_pk_cache SELECT number, [number, number, number] FROM numbers(2100);
        ALTER TABLE test_pk_cache ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
        """)

    time.sleep(2)

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Miss primary key cache")

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Hit primary key cache")


def test_primary_key_cache_int32(started_cluster):
    instance.query("DROP TABLE IF EXISTS test_pk_cache")
    instance.query(
        """
        CREATE TABLE test_pk_cache(id Int32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3)
        engine MergeTree primary key id
        SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1000, enable_primary_key_cache=true;
        INSERT INTO test_pk_cache SELECT number, [number, number, number] FROM numbers(2100);
        ALTER TABLE test_pk_cache ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
        """)

    time.sleep(2)

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Miss primary key cache")

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Hit primary key cache")


def test_primary_key_cache_int64(started_cluster):
    instance.query("DROP TABLE IF EXISTS test_pk_cache")
    instance.query(
        """
        CREATE TABLE test_pk_cache(id Int64, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3)
        engine MergeTree primary key id
        SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1000, enable_primary_key_cache=true;
        INSERT INTO test_pk_cache SELECT number, [number, number, number] FROM numbers(2100);
        ALTER TABLE test_pk_cache ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
        """)

    time.sleep(2)

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Miss primary key cache")

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Hit primary key cache")


def test_primary_key_cache_int128(started_cluster):
    instance.query("DROP TABLE IF EXISTS test_pk_cache")
    instance.query(
        """
        CREATE TABLE test_pk_cache(id Int128, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3)
        engine MergeTree primary key id
        SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1000, enable_primary_key_cache=true;
        INSERT INTO test_pk_cache SELECT number, [number, number, number] FROM numbers(2100);
        ALTER TABLE test_pk_cache ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
        """)

    time.sleep(2)

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Miss primary key cache")

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Hit primary key cache")


def test_primary_key_cache_int256(started_cluster):
    instance.query("DROP TABLE IF EXISTS test_pk_cache")
    instance.query(
        """
        CREATE TABLE test_pk_cache(id Int256, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3)
        engine MergeTree primary key id
        SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1000, enable_primary_key_cache=true;
        INSERT INTO test_pk_cache SELECT number, [number, number, number] FROM numbers(2100);
        ALTER TABLE test_pk_cache ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
        """)

    time.sleep(2)

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Miss primary key cache")

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Hit primary key cache")


def test_primary_key_cache_float32(started_cluster):
    instance.query("DROP TABLE IF EXISTS test_pk_cache")
    instance.query(
        """
        CREATE TABLE test_pk_cache(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3)
        engine MergeTree primary key id
        SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1000, enable_primary_key_cache=true;
        INSERT INTO test_pk_cache SELECT number, [number, number, number] FROM numbers(2100);
        ALTER TABLE test_pk_cache ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
        """)

    time.sleep(2)

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Miss primary key cache")

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Hit primary key cache")


def test_primary_key_cache_float64(started_cluster):
    instance.query("DROP TABLE IF EXISTS test_pk_cache")
    instance.query(
        """
        CREATE TABLE test_pk_cache(id Float64, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3)
        engine MergeTree primary key id
        SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1000, enable_primary_key_cache=true;
        INSERT INTO test_pk_cache SELECT number, [number, number, number] FROM numbers(2100);
        ALTER TABLE test_pk_cache ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
        """)

    time.sleep(2)

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Miss primary key cache")

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Hit primary key cache")


def test_primary_key_cache_date(started_cluster):
    instance.query("DROP TABLE IF EXISTS test_pk_cache")
    instance.query(
        """
        CREATE TABLE test_pk_cache(id Date, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3)
        engine MergeTree primary key id
        SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1000, enable_primary_key_cache=true;
        INSERT INTO test_pk_cache SELECT number + today(), [number, number, number] FROM numbers(2100);
        ALTER TABLE test_pk_cache ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
        """)

    time.sleep(2)

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Miss primary key cache")

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Hit primary key cache")


def test_primary_key_cache_date32(started_cluster):
    instance.query("DROP TABLE IF EXISTS test_pk_cache")
    instance.query(
        """
        CREATE TABLE test_pk_cache(id Date32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3)
        engine MergeTree primary key id
        SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1000, enable_primary_key_cache=true;
        INSERT INTO test_pk_cache SELECT number + today(), [number, number, number] FROM numbers(2100);
        ALTER TABLE test_pk_cache ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
        """)

    time.sleep(2)

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Miss primary key cache")

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Hit primary key cache")


def test_primary_key_cache_datetime(started_cluster):
    instance.query("DROP TABLE IF EXISTS test_pk_cache")
    instance.query(
        """
        CREATE TABLE test_pk_cache(id DateTime, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3)
        engine MergeTree primary key id
        SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1000, enable_primary_key_cache=true;
        INSERT INTO test_pk_cache SELECT number + today(), [number, number, number] FROM numbers(2100);
        ALTER TABLE test_pk_cache ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
        """)

    time.sleep(2)

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Miss primary key cache")

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Hit primary key cache")


def test_primary_key_cache_datetime64(started_cluster):
    instance.query("DROP TABLE IF EXISTS test_pk_cache")
    instance.query(
        """
        CREATE TABLE test_pk_cache(id DateTime64, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3)
        engine MergeTree primary key id
        SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1000, enable_primary_key_cache=true;
        INSERT INTO test_pk_cache SELECT number + today(), [number, number, number] FROM numbers(2100);
        ALTER TABLE test_pk_cache ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
        """)

    time.sleep(2)

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Miss primary key cache")

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Hit primary key cache")

def test_primary_key_cache_enum(started_cluster):
    instance.query("DROP TABLE IF EXISTS test_pk_cache")
    instance.query(
        """
        CREATE TABLE test_pk_cache(id Enum8('a'=1, 'b'=2, 'c'=3, 'd'=4, 'f'=5, 'g'=6, 'h'=7, 'i'=8, 'j'=9, 'k'=10), vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3)
        engine MergeTree primary key id
        SETTINGS index_granularity=1, min_rows_to_build_vector_index=1, enable_primary_key_cache=true;
        INSERT INTO test_pk_cache Values('a', [1, 1, 1]), ('b', [2, 2, 2]), ('c', [3, 3, 3]), ('d', [4, 4, 4]), ('f', [6, 6, 6]), ('g', [7, 7, 7]), ('h', [8, 8, 8]), ('i', [9, 9, 9]),('j', [5, 5, 5]),('k', [10, 10, 10]) ;
        ALTER TABLE test_pk_cache ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
        """)

    time.sleep(2)

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Miss primary key cache")

    instance.query("SELECT id, distance('topK=5')(vector, [0.1, 0.1, 0.1]) FROM test_pk_cache")
    assert instance.contains_in_log("Hit primary key cache")

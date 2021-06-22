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


def test_primary_key_cache_enabled(started_cluster):
    instance.query(
        """
        CREATE TABLE test_pk_cache(id UInt32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3)
        engine MergeTree primary key id
        SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1000, enable_primary_key_cache=true;
        INSERT INTO test_pk_cache SELECT number, [number, number, number] FROM numbers(2100);
        ALTER TABLE test_pk_cache ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
        """)

    time.sleep(2)

    instance.query("SELECT id, distance(vector, [0.1, 0.1, 0.1]) as dist FROM test_pk_cache ORDER BY dist LIMIT 5")
    assert instance.contains_in_log("Miss primary key cache")

    instance.query("SELECT id, distance(vector, [0.1, 0.1, 0.1]) as dist FROM test_pk_cache ORDER BY dist LIMIT 5")
    assert instance.contains_in_log("Hit primary key cache")

    instance.query("DROP TABLE IF EXISTS test_pk_cache")


def test_primary_key_cache_disabled(started_cluster):
    instance.query(
        """
        CREATE TABLE test_pk_cache_disable(id UInt32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3)
        engine MergeTree primary key id
        SETTINGS index_granularity=1024, min_rows_to_build_vector_index=1000, enable_primary_key_cache=false;
        INSERT INTO test_pk_cache_disable SELECT number, [number, number, number] FROM numbers(2100);
        ALTER TABLE test_pk_cache_disable ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
        """)

    time.sleep(2)

    instance.query("SELECT id, distance(vector, [0.1, 0.1, 0.1]) as dist FROM test_pk_cache_disable ORDER BY dist LIMIT 5")
    assert instance.contains_in_log("enable_primary_key_cache = false")

    instance.query("DROP TABLE IF EXISTS test_pk_cache_disable")

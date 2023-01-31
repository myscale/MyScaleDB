import pytest
import time
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_drop_index_cancel_building_index(started_cluster):
    instance.query(
        """
    CREATE TABLE test_drop_index(id UInt32, text String, vector FixedArray(Float32, 768)) Engine MergeTree ORDER BY id;
    INSERT INTO test_drop_index SELECT number, randomPrintableASCII(80), range(768) FROM numbers(50000);
    optimize table test_drop_index final;
    ALTER TABLE test_drop_index ADD VECTOR INDEX v1 vector TYPE HNSWSQ;
    """
    )

    time.sleep(1)

    instance.query("ALTER TABLE test_drop_index DROP VECTOR INDEX v1")

    instance.wait_for_log_line("Vector index has been dropped")
    assert instance.contains_in_log("Vector index has been dropped")

    instance.query("DROP TABLE IF EXISTS test_drop_index")


def test_drop_table_cancel_building_index(started_cluster):
    instance.query(
        """
    CREATE TABLE test_drop_table(id UInt32, text String, vector FixedArray(Float32, 768)) Engine MergeTree ORDER BY id;
    INSERT INTO test_drop_table SELECT number, randomPrintableASCII(80), range(768) FROM numbers(50000);
    optimize table test_drop_table final;
    ALTER TABLE test_drop_table ADD VECTOR INDEX v1 vector TYPE HNSWSQ;
    """
    )

    time.sleep(1)

    instance.query("DROP TABLE test_drop_table SYNC")

    assert instance.contains_in_log("Cancelled building vector index")


def test_drop_table_release_index_cache(started_cluster):
    instance.query(
        """
    CREATE TABLE test_drop_table_release_cache(id UInt32, text String, vector FixedArray(Float32, 3)) Engine MergeTree ORDER BY id;
    INSERT INTO test_drop_table_release_cache SELECT number, randomPrintableASCII(80), range(3) FROM numbers(1000);
    ALTER TABLE test_drop_table_release_cache ADD VECTOR INDEX v1 vector TYPE HNSWSQ;
    """
    )

    instance.wait_for_log_line("index build complete")

    assert instance.query("select status from system.vector_indices where database = currentDatabase() and table = 'test_drop_table_release_cache'") == "Built\n"
    instance.query("DROP TABLE test_drop_table_release_cache SYNC")

    assert instance.contains_in_log("num of cache items after forceExpire 0")

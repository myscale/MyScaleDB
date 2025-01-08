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
    DROP TABLE IF EXISTS test_drop_index;
    CREATE TABLE test_drop_index(id UInt32, text String, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 512) Engine MergeTree ORDER BY id;
    INSERT INTO test_drop_index SELECT number, randomPrintableASCII(80), range(512) FROM numbers(500000);
    optimize table test_drop_index final;
    ALTER TABLE test_drop_index ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
    SYSTEM START BUILD VECTOR INDICES;
    """
    )

    time.sleep(1)

    instance.query("ALTER TABLE test_drop_index DROP VECTOR INDEX v1;")

    instance.wait_for_log_line("Cancelled building vector index")
    assert instance.contains_in_log("Cancelled building vector index")

    instance.query("DROP TABLE IF EXISTS test_drop_index")

def test_drop_table_cancel_building_index(started_cluster):
    instance.query(
        """
    DROP TABLE IF EXISTS test_drop_table;
    CREATE TABLE test_drop_table(id UInt32, text String, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 768) Engine MergeTree ORDER BY id;
    INSERT INTO test_drop_table SELECT number, randomPrintableASCII(80), range(768) FROM numbers(500000);
    optimize table test_drop_table final;
    ALTER TABLE test_drop_table ADD VECTOR INDEX v1 vector TYPE IVFFLAT;
    SYSTEM START BUILD VECTOR INDICES;
    """
    )

    time.sleep(1)

    instance.query("DROP TABLE test_drop_table SYNC;")

    assert instance.contains_in_log("Cancelled building vector index")


def test_drop_table_release_index_cache(started_cluster):
    instance.query(
        """
    CREATE TABLE test_drop_table_release_cache(id UInt32, text String, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) Engine MergeTree ORDER BY id;
    INSERT INTO test_drop_table_release_cache SELECT number, randomPrintableASCII(80), range(3) FROM numbers(1000);
    ALTER TABLE test_drop_table_release_cache ADD VECTOR INDEX v1 vector TYPE HNSWSQ;
    """
    )

    instance.query("SYSTEM WAIT BUILDING VECTOR INDICES test_drop_table_release_cache;")

    assert instance.query("select status from system.vector_indices where database = currentDatabase() and table = 'test_drop_table_release_cache'") == "Built\n"
    instance.query("DROP TABLE test_drop_table_release_cache SYNC")

    assert instance.contains_in_log("Num of cache items after forceExpire 0")


def test_drop_index_cancel_building_mstg_index(started_cluster):
    instance.query(
        """
    DROP TABLE IF EXISTS test_drop_mstg_index;
    CREATE TABLE test_drop_mstg_index(id UInt32, text String, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 512) Engine MergeTree ORDER BY id;
    INSERT INTO test_drop_mstg_index SELECT number, randomPrintableASCII(80), range(512) FROM numbers(500000);
    optimize table test_drop_mstg_index final;
    ALTER TABLE test_drop_mstg_index ADD VECTOR INDEX v1 vector TYPE MSTG;
    """
    )

    instance.wait_for_log_line("MSTG adding data finished")

    instance.query("ALTER TABLE test_drop_mstg_index DROP VECTOR INDEX v1;")

    instance.wait_for_log_line("Aborting the SearchIndex now")
    assert instance.contains_in_log("Aborting the SearchIndex now")

    instance.query("DROP TABLE IF EXISTS test_drop_mstg_index")


def test_drop_table_cancel_building_mstg_index(started_cluster):
    instance.query(
        """
    DROP TABLE IF EXISTS test_drop_mstg_table;
    CREATE TABLE test_drop_mstg_table(id UInt32, text String, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 768) Engine MergeTree ORDER BY id;
    INSERT INTO test_drop_mstg_table SELECT number, randomPrintableASCII(80), range(768) FROM numbers(500000);
    optimize table test_drop_mstg_table final;
    ALTER TABLE test_drop_mstg_table ADD VECTOR INDEX v1 vector TYPE MSTG;
    """
    )

    instance.wait_for_log_line("MSTG adding data finished")

    instance.query("DROP TABLE test_drop_mstg_table SYNC;")

    assert instance.contains_in_log("Aborting the SearchIndex now")

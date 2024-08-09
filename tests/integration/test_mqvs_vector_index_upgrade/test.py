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


def test_decouple_index_upgrade(started_cluster):
    instance.query(
        """
    CREATE TABLE test_decouple_upgrade(id UInt32, text String, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 768) Engine MergeTree ORDER BY id settings enable_rebuild_for_decouple=0;
    INSERT INTO test_decouple_upgrade SELECT number, randomPrintableASCII(80), range(768) FROM numbers(5000);
    INSERT INTO test_decouple_upgrade SELECT number + 5000, randomPrintableASCII(80), range(768) FROM numbers(5000);
    ALTER TABLE test_decouple_upgrade ADD VECTOR INDEX v1 vector TYPE SCANN;
    """
    )

    time.sleep(1)
    
    start = time.time()
    timeout = 60 * 10
    while time.time() - start < timeout:
        if instance.query("select status from system.vector_indices where table = 'test_decouple_upgrade'") == "Built\n":
            break
        time.sleep(10)
    
    instance.query("Optimize table test_decouple_upgrade FINAL")

    data_part_path = "/var/lib/clickhouse/data/default/test_decouple_upgrade/all_1_2_1"
    instance.stop_clickhouse()
    instance.exec_in_container(
        [
            "bash",
            "-c",
            f"""
        rm -rf {data_part_path}/*vector_index_ready_v2.vidx2
            """,
        ]
    )
    instance.exec_in_container(
        [
            "bash",
            "-c",
            f"""
        mv {data_part_path}/merged-0-all_1_1_0-vector_index_description.vidx2 {data_part_path}/merged-0-all_1_1_0-vector_index_ready.vidx2
            """,
        ]
    )
    instance.exec_in_container(
        [
            "bash",
            "-c",
            f"""
        mv {data_part_path}/merged-1-all_2_2_0-vector_index_description.vidx2 {data_part_path}/merged-1-all_2_2_0-vector_index_ready.vidx2
            """,
        ]
    )
    instance.restart_clickhouse()

    assert instance.query("select 1") == "1\n"
    assert instance.contains_in_log("Convert file merged-1-all_2_2_0-vector_index_ready.vidx2 to merged-1-all_2_2_0-vector_index_description.vidx2, and create new ready file merged-1-all_2_2_0-vector_index_ready_v2.vidx2")


def test_single_vector_index_upgrade(started_cluster):
    instance.query(
        """
    CREATE TABLE test_single_index_upgrade(id UInt32, text String, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 768) Engine MergeTree ORDER BY id;
    INSERT INTO test_single_index_upgrade SELECT number, randomPrintableASCII(80), range(768) FROM numbers(5000);
    ALTER TABLE test_single_index_upgrade ADD VECTOR INDEX v1 vector TYPE SCANN;
    """
    )

    start = time.time()
    timeout = 60 * 10
    while time.time() - start < timeout:
        if instance.query("select status from system.vector_indices where table = 'test_single_index_upgrade'") == "Built\n":
            break
        time.sleep(10)
    
    instance.stop_clickhouse()
    data_part_path = "/var/lib/clickhouse/data/default/test_single_index_upgrade/all_1_1_0"
    instance.exec_in_container(
        [
            "bash",
            "-c",
            f"""
        rm -rf {data_part_path}/vector_index_ready_v2.vidx2
            """,
        ]
    )
    
    instance.exec_in_container(
        [
            "bash",
            "-c",
            f"""
        mv {data_part_path}/vector_index_description.vidx2 {data_part_path}/vector_index_ready.vidx2
            """,
        ]
    )
    
    instance.restart_clickhouse()

    assert instance.query("select status from system.vector_indices where table = 'test_single_index_upgrade'") == "Built\n"
    assert instance.contains_in_log("Convert file vector_index_ready.vidx2 to vector_index_description.vidx2, and create new ready file vector_index_ready_v2.vidx2")

def test_reuse_vector_index(started_cluster):
    instance.query(
        """
    CREATE TABLE test_reuse_index(id UInt32, text String, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 768) Engine MergeTree ORDER BY id;
    INSERT INTO test_reuse_index SELECT number, randomPrintableASCII(80), range(768) FROM numbers(5000);
    ALTER TABLE test_reuse_index ADD VECTOR INDEX v1 vector TYPE SCANN;
    """
    )

    start = time.time()
    timeout = 60 * 10
    while time.time() - start < timeout:
        if instance.query("select status from system.vector_indices where table = 'test_reuse_index'") == "Built\n":
            break
        time.sleep(10)
    
    instance.stop_clickhouse()
    
    time.sleep(10)
    
    instance.restart_clickhouse()
    
    assert instance.query("select status from system.vector_indices where table = 'test_reuse_index'") == "Built\n"

    assert instance.contains_in_log("The current version file already exists locally, does not need to convert")

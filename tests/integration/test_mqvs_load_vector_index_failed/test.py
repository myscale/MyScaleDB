import pytest
import time
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", stay_alive=True)
path_to_index_ready_file = "/var/lib/clickhouse/data/default/test_load_vector_index_failed/all_1_1_0/v1-vector_index_description.vidx3"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_load_vector_index_failed(started_cluster):
    instance.query(
        """
    CREATE TABLE test_load_vector_index_failed(id UInt32, text String, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) Engine MergeTree ORDER BY id;
    INSERT INTO test_load_vector_index_failed SELECT number, randomPrintableASCII(80), range(3) FROM numbers(1000);
    ALTER TABLE test_load_vector_index_failed ADD VECTOR INDEX v1 vector TYPE HNSWSQ;
    """
    )

    instance.query("SYSTEM WAIT BUILDING VECTOR INDICES test_load_vector_index_failed;")

    instance.restart_clickhouse()
    time.sleep(3)

    instance.exec_in_container(
        ["bash", "-c", "mv {} {}".format(path_to_index_ready_file, path_to_index_ready_file + ".bak")],
        privileged=True,
        user="root",
    )

    instance.query("SELECT id, vector, distance(vector, [300.0, 300, 300]) AS dist FROM test_load_vector_index_failed ORDER BY dist LIMIT 10;")

    assert instance.contains_in_log("Index is not in the ready state and cannot be loaded")

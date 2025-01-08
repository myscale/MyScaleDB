import logging
import random
import threading
import time
from collections import Counter

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", macros={"cluster": "test1"}, with_zookeeper=True)
node2 = cluster.add_instance("node2", macros={"cluster": "test1"}, with_zookeeper=True)

all_nodes = [node1, node2]


def prepare_cluster():
    for node in all_nodes:
        node.query(
            """
        DROP TABLE IF EXISTS test_vector SYNC;
        CREATE TABLE test_vector(id Int32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 960)
        ENGINE ReplicatedMergeTree('/clickhouse/{cluster}/tables/test/test_vector', '{instance}')
        ORDER BY id;
        """
        )

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_build_index_prevent_merge(started_cluster):
    prepare_cluster()

    node1.query("INSERT INTO test_vector SELECT number, range(960) FROM numbers(100000) SETTINGS min_insert_block_size_rows=1000")
    node1.query("ALTER TABLE test_vector ADD VECTOR INDEX replia_ind vector TYPE HNSWFLAT('metric_type=L2','m=16','ef_c=128');")
    node1.query("OPTIMIZE TABLE test_vector FINAL")

    node2.query("SYSTEM WAIT BUILDING VECTOR INDICES test_vector;")

    assert int(node1.count_in_log("Vector index build is cancelled").strip()) == 0
    assert int(node2.count_in_log("Vector index build is cancelled").strip()) == 0

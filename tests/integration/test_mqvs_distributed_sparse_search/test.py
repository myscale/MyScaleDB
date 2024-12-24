import pytest
import time
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# shard1
node_s1_r1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True)
node_s1_r2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True)

# shard2
node_s2_r1 = cluster.add_instance("node3", main_configs=["configs/remote_servers.xml"], with_zookeeper=True)
node_s2_r2 = cluster.add_instance("node4", main_configs=["configs/remote_servers.xml"], with_zookeeper=True)

data = [
        (1, [(1, 1.0), (2, 1.0), (3, 1.0), (4, 1.0), (5, 1.0), (6, 1.0), (7, 1.0), (8, 1.0), (9, 1.0), (10, 1.0)]),
        (2, [(11, 1.0), (12, 1.0), (13, 1.0), (14, 1.0), (15, 1.0), (16, 1.0), (17, 1.0), (18, 1.0), (19, 1.0), (20, 1.0)]),
        (3, [(21, 1.0), (22, 1.0), (23, 1.0), (24, 1.0), (25, 1.0), (26, 1.0), (27, 1.0), (28, 1.0), (29, 1.0), (30, 1.0)]),
        (4, [(31, 1.0), (32, 1.0), (33, 1.0), (34, 1.0), (35, 1.0), (36, 1.0), (37, 1.0), (38, 1.0), (39, 1.0), (40, 1.0)]),
        (5, [(41, 1.0), (42, 1.0), (43, 1.0), (44, 1.0), (45, 1.0), (46, 1.0), (47, 1.0), (48, 1.0), (49, 1.0), (50, 1.0)]),
        (6, [(51, 1.0), (52, 1.0), (53, 1.0), (54, 1.0), (55, 1.0), (56, 1.0), (57, 1.0), (58, 1.0), (59, 1.0), (60, 1.0)]),
        (7, [(61, 1.0), (62, 1.0), (63, 1.0), (64, 1.0), (65, 1.0), (66, 1.0), (67, 1.0), (68, 1.0), (69, 1.0), (70, 1.0)]),
        (8, [(71, 1.0), (72, 1.0), (73, 1.0), (74, 1.0), (75, 1.0), (76, 1.0), (77, 1.0), (78, 1.0), (79, 1.0), (80, 1.0)]),
        (9, [(81, 1.0), (82, 1.0), (83, 1.0), (84, 1.0), (85, 1.0), (86, 1.0), (87, 1.0), (88, 1.0), (89, 1.0), (90, 1.0)]),
        (10, [(91, 1.0), (92, 1.0), (93, 1.0), (94, 1.0), (95, 1.0), (96, 1.0), (97, 1.0), (98, 1.0), (99, 1.0), (100, 1.0)]),
        (11, [(1, 2.0), (2, 2.0), (3, 2.0), (4, 2.0), (5, 2.0), (6, 2.0), (7, 2.0), (8, 2.0), (9, 2.0), (10, 2.0)]),
        (12, [(11, 2.0), (12, 2.0), (13, 2.0), (14, 2.0), (15, 2.0), (16, 2.0), (17, 2.0), (18, 2.0), (19, 2.0), (20, 2.0)]),
        (13, [(21, 2.0), (22, 2.0), (23, 2.0), (24, 2.0), (25, 2.0), (26, 2.0), (27, 2.0), (28, 2.0), (29, 2.0), (30, 2.0)]),
        (14, [(31, 2.0), (32, 2.0), (33, 2.0), (34, 2.0), (35, 2.0), (36, 2.0), (37, 2.0), (38, 2.0), (39, 2.0), (40, 2.0)]),
        (15, [(41, 2.0), (42, 2.0), (43, 2.0), (44, 2.0), (45, 2.0), (46, 2.0), (47, 2.0), (48, 2.0), (49, 2.0), (50, 2.0)]),
        (16, [(51, 2.0), (52, 2.0), (53, 2.0), (54, 2.0), (55, 2.0), (56, 2.0), (57, 2.0), (58, 2.0), (59, 2.0), (60, 2.0)]),
        (17, [(61, 2.0), (62, 2.0), (63, 2.0), (64, 2.0), (65, 2.0), (66, 2.0), (67, 2.0), (68, 2.0), (69, 2.0), (70, 2.0)]),
        (18, [(71, 2.0), (72, 2.0), (73, 2.0), (74, 2.0), (75, 2.0), (76, 2.0), (77, 2.0), (78, 2.0), (79, 2.0), (80, 2.0)]),
        (19, [(81, 2.0), (82, 2.0), (83, 2.0), (84, 2.0), (85, 2.0), (86, 2.0), (87, 2.0), (88, 2.0), (89, 2.0), (90, 2.0)])
    ]

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in [node_s1_r1, node_s1_r2]:
            node.query(
                """
                CREATE TABLE local_table
                (
                    `id` UInt64,
                    `data` Map(UInt32, Float32)
                )
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_1/local_table', '{replica}')
                ORDER BY id;
                """.format(
                    replica=node.name
                )
            )
        
        for node in [node_s2_r1, node_s2_r2]:
            node.query(
                """
                CREATE TABLE local_table
                (
                    `id` UInt64,
                    `data` Map(UInt32, Float32)
                )
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_2/local_table', '{replica}')
                ORDER BY id;
                """.format(
                    replica=node.name
                )
            )

        node_s1_r1.query("CREATE TABLE distributed_table(id UInt32, data Map(UInt32, Float32)) ENGINE = Distributed(test_cluster, default, local_table, id);")
        node_s1_r1.query("INSERT INTO distributed_table (id, data) VALUES {}".format(", ".join(map(str, data))))

        node_s1_r1.query("ALTER TABLE local_table ON CLUSTER test_cluster ADD INDEX sparse_idx data TYPE sparse;")
        node_s1_r1.query("ALTER TABLE local_table ON CLUSTER test_cluster MATERIALIZE INDEX sparse_idx;")

        # Create a MergeTree table with the same data as the distributed table to serve as the ground truth for queries in the distributed TextSearch.
        node_s1_r1.query("CREATE TABLE sparse_table (id UInt32, data Map(UInt32, Float32)) ENGINE = MergeTree ORDER BY id;")
        node_s1_r1.query("INSERT INTO sparse_table (id, data) VALUES {}".format(", ".join(map(str, data))))
        node_s1_r1.query("ALTER TABLE sparse_table ADD INDEX sparse_idx data TYPE sparse;")
        node_s1_r1.query("ALTER TABLE sparse_table MATERIALIZE INDEX sparse_idx;")

        time.sleep(2)

        yield cluster

    finally:
        cluster.shutdown()

def test_distributed_text_search(started_cluster):

    # Test top 10 results
    assert node_s1_r1.query("""
                            SELECT id, SparseSearch('search_mode=brute_force')(data, map(1, 1.4, 12, 1.6, 23, 2.1, 34, 2.6, 45, 3.1, 56, 3.6, 67, 4.1, 78, 4.6, 89, 5.1, 100, 5.6)) as score
                            FROM distributed_table
                            ORDER BY score DESC
                            LIMIT 10;
                            """) == node_s1_r1.query("""
                                                     SELECT id, SparseSearch('search_mode=brute_force')(data, map(1, 1.4, 12, 1.6, 23, 2.1, 34, 2.6, 45, 3.1, 56, 3.6, 67, 4.1, 78, 4.6, 89, 5.1, 100, 5.6)) as score
                                                     FROM sparse_table
                                                     ORDER BY score DESC
                                                     LIMIT 10;
                                                     """)

    # Test top 20 results
    assert node_s1_r1.query("""
                            SELECT id, SparseSearch('search_mode=brute_force')(data, map(1, 1.4, 12, 1.6, 23, 2.1, 34, 2.6, 45, 3.1, 56, 3.6, 67, 4.1, 78, 4.6, 89, 5.1, 100, 5.6)) as score
                            FROM distributed_table
                            ORDER BY score DESC
                            LIMIT 20;
                            """) == node_s1_r1.query("""
                                                     SELECT id, SparseSearch('search_mode=brute_force')(data, map(1, 1.4, 12, 1.6, 23, 2.1, 34, 2.6, 45, 3.1, 56, 3.6, 67, 4.1, 78, 4.6, 89, 5.1, 100, 5.6)) as score
                                                     FROM sparse_table
                                                     ORDER BY score DESC
                                                     LIMIT 20;
                                                     """)

    # TODO: LWD
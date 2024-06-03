import time
from .utils import logger, is_server_alive, get_md5
import os
import subprocess


def check_all_replicas_vector(client_ls, build_timeout):
    """
    check whether all replicas has already built vector index successfully
    """
    for i in range(len(client_ls) - 1):
        if not client_ls[i].check_vector_build_status(300):
            logger.error("index building failed for replica {} within {}s"
                         .format(i + 1, (i + 1) * 300 + build_timeout))
            exit(-1)


def check_data_integrity(host, output_path="data.csv"):
    """
    select some data and save in output_path, then return its md5sum
    """
    if os.path.exists(output_path):
        os.remove(output_path)

    # use clickhouse-client faster and easier for saving large numbers of data than clickhouse connect
    client_command = f"clickhouse-client -h {host} -q 'select * from gist1m where id < 10000 order by id' > {output_path}"
    subprocess.run(client_command, shell=True, text=True, capture_output=True)
    time.sleep(1)
    return get_md5(output_path)


def check_vector_integrity(host, output_path="vector.csv", query_path="queries/check_vector_integrity.sql"):
    """
    run vector search defined in query_path and save the result in output_path, then return its md5sum
    """
    if os.path.exists(output_path):
        os.remove(output_path)

    # use clickhouse-client faster and easier for running multiple queries defined in a file than clickhouse connect
    client_command = f"clickhouse-client -h {host} --multiquery {query_path} > {output_path}"
    subprocess.run(client_command, shell=True, text=True, capture_output=True)
    time.sleep(1)
    return get_md5(output_path)


def is_cluster_running(host_prefix, replica=2):
    """
    check whether the cluster is running
    """
    for i in range(replica):
        if not is_server_alive(host_prefix + str(i)):
            return False

    return True


def check_replicas_consistency(client, retry=0):
    """
    check whether two replicas have the same number of data
    """
    while retry >= 0:
        try:
            res = client.run_query(
                "select total_rows from clusterAllReplicas('{cluster}', system.tables) where name = 'gist1m'")
            if len(res) != 2:
                logger.warn(f"just got {len(res)} result, probably cluster file not updated")
                return False
            elif res[0][0] != res[1][0]:
                logger.error(f"two replicas have different data {res[0][0]} {res[1][0]}")
                return False
            logger.info(f"two replicas have the same data")
            return True
        except Exception as e:
            logger.error(f"Unexpected exception {str(e)}", exc_info=True)
            retry -= 1

    return False


def check_replicas_consistency_with_timeout(client, timeout):
    """
    check two replicas consistency within timeout
    """
    start = time.time()
    while not check_replicas_consistency(client):
        if time.time() - start > timeout:
            logger.error(f"after {timeout}s, replicas still have different data")
            exit(-1)
        logger.info("replicas still have different data")
        logger.info("sleep 5s and then recheck consistency")
        time.sleep(5)
    pass_time = time.time() - start
    logger.info(f"after {pass_time}s, replicas have same data")


def inject_fault(chaos_client, fault_n, faults_ls):
    """
    apply a chaos fault
    :param chaos_client: k8s chaos mesh client
    :param fault_n: the order of this fault in faults list
    :param faults_ls: a list of faults that contains fault name, namespace and kind.
    these faults are included in the chaos config and defined in chaos-mesh folder.
    """
    name = faults_ls[fault_n]["name"]
    ns = faults_ls[fault_n]["namespace"]
    kind = faults_ls[fault_n]["kind"]

    if chaos_client.exist(name, ns, kind):
        chaos_client.delete(name, ns, kind)
        logger.info(f"delete {kind} {name} from namespace {ns}")
        time.sleep(5)

    # inject fault
    retry_num = 0
    while retry_num < 10:
        # could have conflicts
        try:
            chaos_client.create(name, ns, kind)
            logger.info(f"create {kind} {name} on namespace {ns}")
            break
        except Exception:
            time.sleep(2)
            retry_num += 1

    return faults_ls[fault_n]["qps_timeout"]


def insert_data_during_chaos(host_prefix, timeout, client, all_chaos):
    """
    insert batch data into cluster until cluster running
    """
    start = time.time()
    # insert batch beginning id
    start_id = 500000
    i = 0
    while (i < 10) or (not is_cluster_running(host_prefix)):

        if time.time() - start > timeout:
            logger.error(f"after {timeout}s, still has unavailable pod")
            exit(1)

        if not all_chaos:
            logger.info("insert 1000 data")
            client.insert_batch(start_id, 1000)
            start_id += 1000
            time.sleep(1)
        i += 1


def insert_data_during_update_chi(timeout, client, chaos_client, config):
    """
    insert batch data into cluster until chi completed
    """
    start = time.time()
    start_id = 500000
    i = 0
    while (i < 10) or (not chaos_client.is_chi_completed(config.chi_name, config.namespace)):
        if time.time() - start > timeout:
            logger.error(f"after {timeout}s, still has unavailable pod")
            exit(1)

        logger.info("insert 1000 data")
        client.insert_batch(start_id, 1000)
        start_id += 1000
        time.sleep(1)
        i += 1


def clear_insert_data(client):
    """
    delete insert data during test
    """
    for _ in range(10):
        try:
            client.run_query(f"delete from {client.table_name} where id >= 500000")
            break
        except Exception as e:
            logger.error(f"Unexpected exception {str(e)}", exc_info=True)
            logger.info("retry after one second")
            time.sleep(1)


def scale_up_cluster_and_check(config, chaos_client, client, timeout):
    """
    scale up cluster and check its status
    """
    chaos_client.scale_up(config.chi_name, config.namespace)
    logger.info("start inject data during scale up")
    insert_data_during_update_chi(timeout, client, chaos_client, config)
    logger.info("cluster is running and finish operation during scale up")
    check_replicas_consistency_with_timeout(client, 300)
    # clear insert test data
    clear_insert_data(client)
    logger.info("finish checking scale up cluster")


def scale_down_cluster_and_check(config, chaos_client, client, timeout):
    """
    scale down cluster and check its status
    """
    chaos_client.scale_down(config.chi_name, config.namespace)
    logger.info("start inject data during scale down")
    insert_data_during_update_chi(timeout, client, chaos_client, config)
    # clear insert test data
    clear_insert_data(client)
    logger.info("finish checking scale down cluster")


def update_cluster_image_and_check(config, chaos_client, client, timeout, image_tag):
    """
    upgrade cluster and check its status
    """
    chaos_client.update_image(config.chi_name, config.namespace, image_tag)
    logger.info("start inject data during upgrade")
    insert_data_during_update_chi(timeout, client, chaos_client, config)
    logger.info("cluster is running and finish operation during upgrade")
    check_replicas_consistency_with_timeout(client, 300)
    # clear insert test data
    clear_insert_data(client)
    logger.info("finish checking upgrade cluster")

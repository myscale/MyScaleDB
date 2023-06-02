import time
from .utils import logger, is_server_alive
from .chaos import PodKill, PodFailure, NetWorkPartition


def check_all_replicas_vector(client_ls, build_timeout):
    """
    check whether all replicas has already built vector index successfully
    """
    for i in range(len(client_ls) - 1):
        if not client_ls[i].check_vector_build_status(300):
            logger.error("index building failed for replica {} within {}s"
                         .format(i + 1, (i + 1) * 300 + build_timeout))
            exit(1)


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


def run_performance_check(faults_ls, fault_interval, chaos_client, prom_client):
    """
    check whether all replicas QPS could recover from faults after {qps_timeout}s defined in chaos config
    """
    for i in range(len(faults_ls)):
        expected_qps = prom_client.query()
        time.sleep(inject_fault(chaos_client, i, faults_ls))
        curr_qps = prom_client.query()
        for key, value in expected_qps.items():
            if key not in curr_qps.keys():
                logger.error(f"replica {key} qps failed to recover")
            else:
                logger.info(f"replica {key} current qps is {curr_qps[key]} and expected {value}")
                if value - curr_qps[key] > 30:
                    logger.error(f"replica {key} qps failed to recover")
                logger.info(f"replica {key} successfully recover within expected time")

        logger.info(f"sleep {fault_interval}s")
        time.sleep(fault_interval)


def run_consistency_check(faults_ls, fault_interval, chaos_client, client_ls, host_prefix):
    """
    check structural data and vector integrity and consistency
    """
    for i in range(len(faults_ls)):
        timeout = inject_fault(chaos_client, i, faults_ls)

        fault_name = faults_ls[i]["name"]
        if "all" in fault_name:
            all_chaos = True
        else:
            all_chaos = False

        if "failure" in fault_name:
            chaos = PodFailure(client_ls, host_prefix, timeout + 120, all_chaos)
        elif "kill" in fault_name:
            chaos = PodKill(client_ls, host_prefix, timeout + 120, all_chaos)
        elif "network" in fault_name:
            chaos = NetWorkPartition(client_ls, host_prefix, timeout + 120, all_chaos)

        # before chaos
        chaos.before_chaos()
        # during chaos
        chaos.during_chaos()
        # after chaos
        chaos.after_chaos()

        logger.info(f"sleep {fault_interval}s")
        time.sleep(fault_interval)


def scale_up_and_check(config, chaos_client, client_ls):
    """
    scale up cluster and check its status
    """
    chaos_client.scale_up(config.chi_name, config.namespace)

    # check new replica running
    begin = time.time()
    while True:
        if is_server_alive(client_ls[-2].host):
            logger.info("new replica running")
            break
        elif time.time() - begin > 600:
            logger.error("after 600s, new replica is still not running")
            exit(-1)
        logger.info("sleep 5s and wait for new replica running")
        time.sleep(5)

    # check the process of new replica synchronizing data
    begin = time.time()
    while client_ls[-2].select_count() != client_ls[0].select_count():
        if time.time() - begin > config.sync_timeout:
            logger.error(f"after {config.sync_timeout}s, new replica still not finish syncing data")
            exit(-1)
        logger.info(f"new replica has {client_ls[-2].select_count()} data now, expected {client_ls[0].select_count()}")
        time.sleep(5)
    logger.info(f"after {time.time() - begin}s, new replica complete synchronizing data process")

    while not chaos_client.is_chi_completed(config.chi_name, config.namespace):
        logger.info("sleep 5s and wait chi completed")
        time.sleep(5)


def scale_down_and_check(config, chaos_client):
    """
    scale down cluster and check its status
    """
    chaos_client.scale_down(config.chi_name, config.namespace)
    logger.info("sleep 180s and wait for cluster scaling down")
    time.sleep(180)

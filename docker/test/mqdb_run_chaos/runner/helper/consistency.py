import time
from .utils import convert_faults_ls, logger
from .chaos import Chaos
from .worker import scale_down_cluster_and_check, scale_up_cluster_and_check, \
    update_cluster_image_and_check, check_all_replicas_vector
import random


class DataConsistencyChecker:

    def __init__(self, config, client_ls, chaos_client, timeout):
        self.config = config
        self.client_ls = client_ls
        self.chaos_client = chaos_client
        self.faults_ls = convert_faults_ls(config.faults_ls)
        self.timeout = timeout

    def run(self):
        """
        inject faults and check structural data and vector integrity and consistency
        """

        logger.info("insert 50w data and build vector index")
        self.client_ls[-1].init_server(500000)
        start = time.time()
        # run scale up, scale down and upgrade cluster test two times
        for i in range(2):
            if i > 0:
                update_cluster_image_and_check(self.config, self.chaos_client,
                                               self.client_ls[-1], 900, self.config.base_image)
                time.sleep(60)

            scale_up_cluster_and_check(self.config, self.chaos_client, self.client_ls[-1], 900)
            # sleep in case clickhouse operator not create chi service
            time.sleep(100)
            scale_down_cluster_and_check(self.config, self.chaos_client, self.client_ls[-1], 300)
            time.sleep(100)
            scale_up_cluster_and_check(self.config, self.chaos_client, self.client_ls[-1], 900)
            time.sleep(100)
            update_cluster_image_and_check(self.config, self.chaos_client, self.client_ls[-1], 900,
                                           self.config.upgrade_image)
            time.sleep(60)

        # inject random chaos within timeout
        while time.time() - start < self.timeout:
            fault_id = random.randint(0, len(self.faults_ls)-1)
            fault_name = self.faults_ls[fault_id]["name"]
            chaos_timeout = self.faults_ls[fault_id]["qps_timeout"]

            if ("one" in fault_name) and ("keeper" not in fault_name):
                all_chaos = False
            else:
                all_chaos = True

            chaos = Chaos(self.client_ls, self.config.host_prefix, fault_name, chaos_timeout + 300,
                          all_chaos, self.faults_ls, fault_id, self.chaos_client)
            chaos.run()

            logger.info(f"sleep {self.config.fault_interval}s")
            time.sleep(self.config.fault_interval)

        logger.info("finish consistency check")
        logger.info("start build vector index for qps performance check")
        self.client_ls[-1].init_server()
        check_all_replicas_vector(self.client_ls, self.config.build_timeout)

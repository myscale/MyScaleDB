from .client import PromClient
from .worker import inject_fault
from .utils import logger, convert_faults_ls
import time


class PerformanceChecker:
    """
    a class used to check whether all replicas QPS could recover from faults
    after {qps_timeout}s defined in chaos config
    """

    def __init__(self, config, chaos_client):
        self.config = config
        self.chaos_client = chaos_client
        self.prom_client = PromClient(config.prom_host, config.namespace, config.chi_name, config.cluster_name)
        self.faults_ls = convert_faults_ls(config.faults_ls)

    def run(self):
        logger.info("run qps performance check")
        for i in range(len(self.faults_ls)):
            expected_qps = self.prom_client.query()
            time.sleep(inject_fault(self.chaos_client, i, self.faults_ls))
            curr_qps = self.prom_client.query()
            for key, value in expected_qps.items():
                if key not in curr_qps.keys():
                    logger.error(f"replica {key} qps failed to recover")
                    exit(-1)
                else:
                    logger.info(f"replica {key} current qps is {curr_qps[key]} and expected {value}")
                    if value - curr_qps[key] > 100:
                        logger.error(f"replica {key} qps failed to recover")
                        exit(-1)
                    logger.info(f"replica {key} successfully recover within expected time")

            logger.info(f"sleep {self.config.fault_interval}s")
            time.sleep(self.config.fault_interval)
        logger.info("finish qps performance check")

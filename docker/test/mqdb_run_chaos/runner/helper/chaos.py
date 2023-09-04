import time
from .utils import logger
from .worker import check_data_integrity, check_vector_integrity, clear_insert_data, \
    check_replicas_consistency_with_timeout, inject_fault, insert_data_during_chaos


class Chaos(object):

    def __init__(self, client_ls, host_prefix, chaos_name, wait_time, all_chaos, faults_ls, fault_id, chaos_client):
        self.client_ls = client_ls
        self.host_prefix = host_prefix
        self.chaos_name = chaos_name
        self.wait_time = wait_time
        self.all_chaos = all_chaos
        self.expected_data_md5 = 0
        self.expected_vector_md5_ls = []
        self.faults_ls = faults_ls
        self.fault_id = fault_id
        self.chaos_client = chaos_client

    def before_chaos(self):
        logger.info("start record data and vector index md5 sum before injecting fault")
        for i in range(len(self.client_ls) - 1):
            host = self.host_prefix + str(i)
            if i == 0:
                self.expected_data_md5 = check_data_integrity(host)
            vector_md5 = check_vector_integrity(host)
            self.expected_vector_md5_ls.append(vector_md5)

    def during_chaos(self):
        logger.info(f"start doing operation during {self.chaos_name}")
        insert_data_during_chaos(self.host_prefix, self.wait_time, self.client_ls[-1], self.all_chaos)
        logger.info("cluster is running and finish operation during chaos")

    def after_chaos(self):
        # check two replicas data consistency and integrity
        logger.info("check data consistency and integrity of two replicas")
        check_replicas_consistency_with_timeout(self.client_ls[-1], 300)
        time.sleep(3)
        self.check_integrity()
        # clear insert test data
        clear_insert_data(self.client_ls[-1])
        logger.info(f"complete check for {self.chaos_name}")

    def run(self):
        self.before_chaos()
        inject_fault(self.chaos_client, self.fault_id, self.faults_ls)
        # sleep three seconds to wait faults work
        time.sleep(3)
        self.during_chaos()
        self.after_chaos()

    def check_integrity(self):
        logger.info("start check data and vector index md5 after recovering from faults")
        for i in range(len(self.client_ls) - 1):
            host = self.host_prefix + str(i)

            # check data integrity
            logger.info(f"check data integrity for {host}")
            data_md5 = check_data_integrity(host)
            if data_md5 != self.expected_data_md5:
                logger.error(f"{host} data md5 changed after chaos, "
                             f"it was {self.expected_data_md5} and now is {data_md5}")

            # check vector search integrity
            if self.all_chaos:
                logger.info(f"check vector search integrity for {host}")
                vector_md5 = check_vector_integrity(host)
                if vector_md5 != self.expected_vector_md5_ls[i]:
                    logger.error(f"{host} vector search result md5 changed after chaos, "
                                 f"it was {self.expected_data_md5} and now is {data_md5}")


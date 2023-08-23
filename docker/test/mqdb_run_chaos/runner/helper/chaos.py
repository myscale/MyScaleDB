import time
import os
import subprocess
from .utils import logger, get_md5, is_server_alive


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


def are_replicas_sync(client_ls):
    """
    check whether all replicas have same data count
    """
    expected = client_ls[-1].select_count()
    for i in range(len(client_ls) - 1):
        count = client_ls[i].select_count()
        if count != expected:
            logger.warning(f"replica {i} has {count} data, but expected {expected}")
            return False

    return True


class BaseChaos(object):

    def __init__(self, client_ls, host_prefix, wait_time, all_chaos):
        self.client_ls = client_ls
        self.host_prefix = host_prefix
        self.wait_time = wait_time
        self.all_chaos = all_chaos
        self.expected_data_md5 = 0
        self.expected_vector_md5_ls = []

    def before_chaos(self):
        logger.info("start record data and vector index md5 sum before injecting fault")
        for i in range(len(self.client_ls) - 1):
            host = self.host_prefix + str(i)
            if i == 0:
                self.expected_data_md5 = check_data_integrity(host)
            vector_md5 = check_vector_integrity(host)
            self.expected_vector_md5_ls.append(vector_md5)

    def during_chaos(self):
        # chaos class need to implement this
        pass

    def after_chaos(self):
        start = time.time()
        ready = False
        while True:
            for i in range(len(self.client_ls) - 1):
                host = self.host_prefix + str(i)
                if not is_server_alive(host):
                    break
                if i == len(self.client_ls) - 2:
                    ready = True

            if ready:
                pass_time = time.time() - start
                logger.info(f"after {pass_time}s, all pods are alive now")
                break

            if time.time() - start > self.wait_time:
                logger.error(f"after {self.wait_time}s, still has unavailable pod")
                exit(1)

            logger.info("sleep 3s and then recheck pod status")
            time.sleep(3)

        self.check_consistency()
        time.sleep(3)
        self.check_integrity()
        if not self.all_chaos:
            for _ in range(10):
                try:
                    self.client_ls[-1].run_query(f"delete from {self.client_ls[-1].table_name} {self.insert_data_filter}")
                    break
                except Exception as e:
                    logger.error(f"Unexpected exception {str(e)}", exc_info=True)
                    logger.info("retry after one second")
                time.sleep(1)

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
            logger.info(f"check vector search integrity for {host}")
            vector_md5 = check_vector_integrity(host)
            if vector_md5 != self.expected_vector_md5_ls[i]:
                logger.error(f"{host} vector search result md5 changed after chaos, "
                             f"it was {self.expected_data_md5} and now is {data_md5}")

    def check_consistency(self):
        start = time.time()
        while not are_replicas_sync(self.client_ls):
            if time.time() - start > 120:
                logger.error(f"after 2m, replicas still have different data")
                exit(1)
            logger.info("replicas still have different data")
            logger.info("sleep 1s and then recheck consistency")
            time.sleep(1)
        pass_time = time.time() - start
        logger.info(f"after {pass_time}s, replicas have same data")


class PodFailure(BaseChaos):

    def __init__(self, client_ls, host_prefix, wait_time, all_chaos,
                 insert_data_filter="where id >= 500000"):
        super().__init__(client_ls, host_prefix, wait_time, all_chaos)
        self.insert_data_filter = insert_data_filter

    def during_chaos(self):
        if self.all_chaos:
            logger.info("do nothing when all pod failed")
            return
        client = self.client_ls[-1]
        logger.info("insert 50w data during pod failure")
        try:
            client.insert_data(self.insert_data_filter)
        except Exception as e:
            logger.error(f"Unexpected exception {str(e)}", exc_info=True)
        logger.info("execute delete from during pod failure")
        # catch mutation not finished error
        try:
            client.run_query(client.delete_from)
        except Exception as e:
            logger.error(f"Unexpected exception {str(e)}", exc_info=True)
        logger.info("finish operation during pod failure")
        time.sleep(30)

    def after_chaos(self):
        super().after_chaos()
        logger.info("complete check for pod failure chaos")


class PodKill(BaseChaos):
    def __init__(self, client_ls, host_prefix, wait_time, all_chaos,
                 insert_data_filter="where id >= 500000"):
        super().__init__(client_ls, host_prefix, wait_time, all_chaos)
        self.wait_time = wait_time
        self.insert_data_filter = insert_data_filter

    def during_chaos(self):
        if self.all_chaos:
            logger.info("do nothing when all pod kill")
            return
        client = self.client_ls[-1]
        logger.info("insert 50w data during pod kill")
        try:
            client.insert_data(self.insert_data_filter)
        except Exception as e:
            logger.error(f"Unexpected exception {str(e)}", exc_info=True)
        logger.info("finish operation during pod kill")

    def after_chaos(self):
        super().after_chaos()
        logger.info("complete check for pod kill chaos")


class NetWorkPartition(BaseChaos):
    def __init__(self, client_ls, host_prefix, wait_time, all_chaos,
                 insert_data_filter="where id >= 500000"):
        super().__init__(client_ls, host_prefix, wait_time, all_chaos)
        self.wait_time = wait_time
        self.insert_data_filter = insert_data_filter

    def during_chaos(self):
        client = self.client_ls[-1]
        logger.info("insert 50w data during network partition")
        try:
            client.insert_data(self.insert_data_filter)
        except Exception as e:
            logger.error(f"Unexpected exception {str(e)}", exc_info=True)
        logger.info("finish operation during network partition")

    def after_chaos(self):
        super().after_chaos()
        logger.info("complete check for network partition")

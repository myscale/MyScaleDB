import os.path
import time
import numpy as np
from .utils import logger
import clickhouse_connect
from prometheus_api_client import PrometheusConnect
import yaml
from kubernetes import config, dynamic
from kubernetes.client import api_client
from kubernetes.dynamic.exceptions import NotFoundError


class DBClient:
    def __init__(self,
                 host,
                 port=8123,
                 username="default",
                 password="",
                 table_name=None,
                 vector_dimension=None,
                 table_ddl=None,
                 insert_data_sql=None,
                 vector_index_sql=None,
                 delete_from=None,
                 build_timeout=None):

        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.table_name = table_name
        self.vector_dimension = vector_dimension
        self.table_ddl = table_ddl
        self.insert_data_sql = insert_data_sql
        self.vector_index_sql = vector_index_sql
        self.build_timeout = build_timeout
        self.delete_from = delete_from

    def get_client(self):
        return clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password
        )

    def run_query(self, query):
        logger.info("run query %s", query)
        res = self.get_client().query(query).result_rows
        return res

    def select_count(self, select_filter=""):
        select_query = f"select count(*) from {self.table_name}"
        if len(select_filter) > 0:
            select_query += select_filter
        return self.run_query(select_query)[0][0]

    def insert_data(self, insert_filter):
        insert_query = self.insert_data_sql
        if len(insert_filter) > 0:
            insert_query += insert_filter
        self.run_query(insert_query)

    def insert_batch(self, start_id, batch_size):
        data_batch = []
        for i in range(batch_size):
            vector = np.random.rand(self.vector_dimension).tolist()
            data_batch.append([start_id + i, vector])
        try:
            self.get_client().insert(self.table_name, data_batch, column_names=['id', 'vector'])
        except Exception as e:
            logger.error("failed to insert batch data, got error {}".format(str(e)))

    def check_vector_build_status(self, timeout):
        check_query = "select table, status from system.vector_indices"
        check_time = 0
        while check_time < timeout:
            check_res = self.run_query(check_query)
            logger.info("build status: {}, check time: {}".format(check_res, check_time))
            for it in check_res:
                if it[1] == "Built":
                    logger.info("index build ready!")
                    return True
            check_time = check_time + 120
            time.sleep(120)

        return False

    def init_server(self, insert_data_length=0):
        logger.info("init server")
        self.run_query("drop table if exists {} sync".format(self.table_name))

        logger.info("create table")
        self.run_query(self.table_ddl)

        logger.info("insert data")
        insert_filter = ""
        if insert_data_length > 0:
            insert_filter = f" where id < {str(insert_data_length)}"
        self.insert_data(insert_filter)
        time.sleep(30)

        logger.info("optimize table")
        self.run_query(
            "optimize table {} final".format(self.table_name))
        time.sleep(30)

        logger.info("create vector index")
        self.run_query(self.vector_index_sql)
        logger.info("check index build status")
        is_ready = self.check_vector_build_status(self.build_timeout)
        if not is_ready:
            logger.error("index building failed within {}s".format(self.timeout))
            exit(1)


class PromClient:
    def __init__(self, host, ns, chi_name, cluster_name):
        try:
            self.client = PrometheusConnect(url=host, disable_ssl=True)
            self.ns = ns
            self.chi_name = chi_name
            self.cluster_name = cluster_name
        except Exception as e:
            logger.error("failed to connect to prometheus, got error {}".format(str(e)))
            exit(1)

    def query(self):
        result = {}
        query = 'rate(ClickHouseProfileEvents_Query{{namespace=~"({})",clickhouse_altinity_com_chi=~"({})",' \
                'clickhouse_altinity_com_cluster=~"({})"}}[1m])'.format(self.ns, self.chi_name, self.cluster_name)
        query_result = self.client.custom_query(query)

        for i in range(len(query_result)):
            replica = query_result[i]["metric"]["clickhouse_altinity_com_replica"]
            qps = float(query_result[i]['value'][1])
            if replica not in result.keys():
                result[replica] = qps
            else:
                result[replica] = max(qps, result[replica])

        return result


class ChaosClient:
    def __init__(self):
        self.client = dynamic.DynamicClient(
            api_client.ApiClient(configuration=config.load_incluster_config())
        )
        self.api_group = {
            "PodChaos": self.client.resources.get(api_version="chaos-mesh.org/v1alpha1", kind="PodChaos"),
            "TimeChaos": self.client.resources.get(api_version="chaos-mesh.org/v1alpha1", kind="TimeChaos"),
            "NetworkChaos": self.client.resources.get(api_version="chaos-mesh.org/v1alpha1", kind="NetworkChaos"),
            "Cluster": self.client.resources.get(api_version="clickhouse.altinity.com/v1",
                                                 kind="ClickHouseInstallation"),
        }

    def create(self, name, namespace, kind):
        with open(os.path.join('chaos-mesh', name + '.yaml')) as f:
            fault = yaml.safe_load(f)
            resp = self.api_group[kind].create(body=fault, namespace=namespace)
            return resp

    def delete(self, name, namespace, kind):
        resp = self.api_group[kind].delete(name=name, namespace=namespace)
        return resp

    def exist(self, name, namespace, kind):
        try:
            self.api_group[kind].get(name=name, namespace=namespace)
        except NotFoundError:
            return False

        return True

    def scale_up(self, name, namespace):
        for _ in range(10):
            try:
                chi = self.api_group["Cluster"].get(name=name, namespace=namespace)
                if chi.spec.configuration.clusters[0].layout.replicasCount == 1:
                    chi.spec.configuration.clusters[0].layout.replicasCount += 1
                    self.api_group["Cluster"].patch(body=chi, content_type="application/merge-patch+json")
                break
            except Exception as e:
                logger.warn(str(e))
                logger.info("retry scale up")

    def scale_down(self, name, namespace):
        for _ in range(10):
            try:
                chi = self.api_group["Cluster"].get(name=name, namespace=namespace)
                if chi.spec.configuration.clusters[0].layout.replicasCount == 2:
                    chi.spec.configuration.clusters[0].layout.replicasCount -= 1
                    self.api_group["Cluster"].patch(body=chi, content_type="application/merge-patch+json")
                break
            except Exception as e:
                logger.warn(str(e))
                logger.info("retry scale down")

    def update_image(self, name, namespace, image_tag):
        for _ in range(10):
            try:
                chi = self.api_group["Cluster"].get(name=name, namespace=namespace)
                if chi.spec.templates.podTemplates[0].spec.containers[0].name == "clickhouse":
                    chi.spec.templates.podTemplates[0].spec.containers[0].image = "harbor.internal.moqi.ai/mqdb/mqdb:" + str(image_tag)
                    self.api_group["Cluster"].patch(body=chi, content_type="application/merge-patch+json")
                break
            except Exception as e:
                logger.warn(str(e))
                logger.info("retry update image")

    def is_chi_completed(self, name, namespace):
        return self.api_group["Cluster"].get(name=name, namespace=namespace).status.status == "Completed"

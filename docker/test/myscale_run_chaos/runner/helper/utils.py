import os
import logging
import yaml
from datetime import timedelta
import re
import hashlib
import requests

UNITS = {'s': 'seconds', 'm': 'minutes', 'h': 'hours', 'd': 'days', 'w': 'weeks'}
LOG_FMT = '%(asctime)s [ %(process)d ] %(levelname)s : %(message)s (%(filename)s:%(lineno)s, %(funcName)s)'
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
logger = logging.getLogger()


class Config:
    def __init__(self, config_path):
        self.config_file = parse_yml_config(config_path)
        # server config
        self.server_config = self.config_file["server"]
        self.namespace = self.server_config["namespace"]
        self.replica = int(self.server_config["replica"])
        self.chi_name = self.server_config["chi_name"]
        self.cluster_name = self.server_config["cluster_name"]
        self.host_prefix = "chi-" + self.chi_name + "-" + self.cluster_name + "-0-"
        self.port = self.server_config["port"]
        self.user = self.server_config["username"]
        self.password = self.server_config["password"]
        self.base_image = self.server_config["base_image"]
        self.upgrade_image = self.server_config["upgrade_image"]

        # queries
        self.queries = self.config_file["queries"]
        self.create_table = self.queries["create_table"]
        self.insert_data = self.queries["insert_data"]
        self.build_index = self.queries["build_index"]
        self.delete_from = self.queries["delete_from"]

        # chaos config
        self.chaos_config = self.config_file["chaos"]
        self.fault_interval = convert_to_seconds(self.chaos_config["fault_interval"])
        self.faults_ls = self.chaos_config["faults"]

        # others
        self.table_name = self.config_file["table_name"]
        self.build_timeout = convert_to_seconds(self.config_file["build_index_timeout"])
        self.vector_dimension = int(self.config_file["vector_dimension"])
        self.prom_host = self.config_file["prom_host"]
        self.chaos_timeout = convert_to_seconds(self.config_file["chaos_timeout"])


def convert_to_seconds(s):
    return int(timedelta(**{
        UNITS.get(m.group('unit').lower(), 'seconds'): float(m.group('val'))
        for m in re.finditer(r'(?P<val>\d+(\.\d+)?)(?P<unit>[smhdw]?)', s, flags=re.I)
    }).total_seconds())


def parse_yml_config(config_path):
    if not os.path.exists(config_path):
        logger.error("{} file not exists".format(config_path))
        exit(1)

    with open(config_path) as f:
        return yaml.safe_load(f)


def convert_faults_ls(original_ls):
    new_ls = []
    for i in range(len(original_ls)):
        fault_dict = {}
        file_name = original_ls[i]["name"]
        fault_yml = parse_yml_config(os.path.join("chaos-mesh", file_name + ".yaml"))
        fault_dict["name"] = fault_yml["metadata"]["name"]
        fault_dict["namespace"] = fault_yml["metadata"]["namespace"]
        fault_dict["kind"] = fault_yml["kind"]
        fault_dict["qps_timeout"] = convert_to_seconds(original_ls[i]["qps_timeout"])
        new_ls.append(fault_dict)

    return new_ls


def get_md5(file_name):
    with open(file_name, 'rb') as f:
        data = f.read()
        md5 = hashlib.md5(data).hexdigest()

    return md5


def is_server_alive(host):
    url = "http://" + host + ":8123/ping"
    try:
        res = requests.get(url)
        return res.status_code == 200
    except requests.exceptions.ConnectionError:
        return False

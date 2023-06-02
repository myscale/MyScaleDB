import argparse
from helper.client import ChaosClient, PromClient
from helper.utils import Config, convert_faults_ls, logger
from helper.worker import run_performance_check

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="run chaos test")
    parser.add_argument(
        "--config-file",
        default="config.yaml",
        help="chaos test config file path"
    )

    args = parser.parse_args()

    # get fault list and configs
    config = Config(args.config_file)
    faults_ls = convert_faults_ls(config.faults_ls)

    # init chaos mesh client
    chaos_client = ChaosClient()

    # init prometheus client
    prom_client = PromClient(config.prom_host, config.namespace, config.chi_name, config.cluster_name)

    logger.info("run qps performance check")
    run_performance_check(faults_ls, config.fault_interval, chaos_client, prom_client)
    logger.info("finish performance check")

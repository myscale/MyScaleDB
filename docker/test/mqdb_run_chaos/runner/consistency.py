import argparse
from helper.client import ChaosClient, DBClient
from helper.utils import Config, convert_faults_ls, logger
from helper.worker import check_all_replicas_vector, run_consistency_check, scale_up_and_check, scale_down_and_check

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

    # init db clients
    logger.info("init db clients")
    client_ls = []
    for i in range(config.replica + 1):
        if i < config.replica:
            host = config.host_prefix + str(i)
        else:
            host = config.cluster_name + "-" + config.chi_name
        client = DBClient(host, config.port, config.user, config.password, config.table_name,
                          config.create_table, config.insert_data, config.build_index,
                          config.delete_from, config.alter_update, config.build_timeout)
        client_ls.append(client)

    logger.info("insert 50w data and build vector index")
    client_ls[-1].init_server(500000)

    # scale up cluster
    logger.info("scale up cluster and check its status")
    scale_up_and_check(config, chaos_client, client_ls)

    # scale down cluster
    logger.info("scale down cluster and check its status")
    scale_down_and_check(config, chaos_client)

    # scale up cluster again
    logger.info("scale up cluster again and check its status")
    scale_up_and_check(config, chaos_client, client_ls)

    logger.info("run operation consistency and data integrity check")
    run_consistency_check(faults_ls, config.fault_interval, chaos_client, client_ls, config.host_prefix)
    logger.info("finish consistency check")
    logger.info("start build vector index for qps check")
    client_ls[-1].init_server()
    check_all_replicas_vector(client_ls, config.build_timeout)

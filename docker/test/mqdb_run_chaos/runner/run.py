import argparse
from helper.client import ChaosClient, DBClient
from helper.utils import Config
from helper.consistency import DataConsistencyChecker
from helper.performance import PerformanceChecker
from helper.worker import check_replicas_consistency

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="run chaos test")
    parser.add_argument(
        "--config-file",
        default="config.yaml",
        help="chaos test config file path"
    )

    subparsers = parser.add_subparsers(title='subcommands', dest='command')
    parser_consistency = subparsers.add_parser('consistency', help='run data consistency check after injecting '
                                                                   'multi-types of chaos')
    parser_performance = subparsers.add_parser('performance', help='run qps performance check after injecting '
                                                                   'multi-types of chaos')
    parser_check = subparsers.add_parser('check', help='check data consistency for two replicas')
    args = parser.parse_args()

    # init configs
    config = Config(args.config_file)
    # init chaos mesh client
    chaos_client = ChaosClient()
    # init db clients
    client_ls = []
    for i in range(config.replica + 1):
        if i < config.replica:
            host = config.host_prefix + str(i)
        else:
            host = config.cluster_name + "-" + config.chi_name
        client = DBClient(host, config.port, config.user, config.password, config.table_name,
                          config.vector_dimension, config.create_table, config.insert_data, config.build_index,
                          config.delete_from, config.build_timeout)
        client_ls.append(client)

    if args.command == 'consistency':
        consistency = DataConsistencyChecker(config, client_ls, chaos_client, config.chaos_timeout)
        consistency.run()
    elif args.command == 'performance':
        performance = PerformanceChecker(config, chaos_client)
        performance.run()
    else:
        if not check_replicas_consistency(client_ls[-1], retry=5):
            exit(-1)




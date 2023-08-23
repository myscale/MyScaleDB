import argparse
import clickhouse_connect
from helper.utils import logger

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="check data consistency for two replicas")
    parser.add_argument(
        "--cluster-host",
        type=str,
        default="clickhouse-chaos-test",
        help="host"
    )
    parser.add_argument(
        "--cluster-port",
        type=int,
        default=8123,
        help="port"
    )

    args = parser.parse_args()
    client = clickhouse_connect.get_client(
        host=args.cluster_host,
        port=args.cluster_port,
        username='default',
        password=''
    )

    # Check the consistency of data between two replicas
    # And add retry just in case network error
    retry = 10
    while retry > 0:
        try:
            res = client.query(
                "select total_rows from clusterAllReplicas('{cluster}', system.tables) where name = 'gist1m'").result_rows
            if len(res) != 2 or res[0][0] != res[1][0]:
                logger.error(f"two replicas have different data {res[0][0]} {res[1][0]}")
                exit(-1)
            logger.info(f"two replicas have the same data")
            break
        except Exception as e:
            logger.error(f"Unexpected exception {str(e)}", exc_info=True)
            retry -= 1

    if retry == 0:
        logger.error("try ten times, still got error")
        exit(-1)

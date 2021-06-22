#!/bin/bash

set -x
PROJECT_PATH=$1
SHA_TO_TEST=$2
WORKPATH=$PROJECT_PATH/docker/test/mqdb_run_smoke
mkdir -p $WORKPATH/workspace
cd $WORKPATH/workspace

dpkg -i $WORKPATH/packages/clickhouse-common-static_*$arch.deb
dpkg -i $WORKPATH/packages/clickhouse-common-static-dbg_*$arch.deb
dpkg -i $WORKPATH/packages/clickhouse-server_*$arch.deb
dpkg -i $WORKPATH/packages/clickhouse-client_*$arch.deb

run_server() {
    rm -rf /etc/clickhouse-server/config.d/listen.xml
    echo '<clickhouse><listen_host>0.0.0.0</listen_host></clickhouse>' >>/etc/clickhouse-server/config.d/listen.xml
    sudo clickhouse start
}

run_client() {
    for i in {1..100}; do
        sleep 1
        clickhouse-client --query "select 'OK'" > $WORKPATH/test_output/run.log 2> $WORKPATH/test_output/clientstderr.log && break
        [[ $i == 100 ]] && echo 'FAIL'
    done
}

run_server
run_client
mv /var/log/clickhouse-server/clickhouse-server.log $WORKPATH/test_output/clickhouse-server.log
$WORKPATH/process_split_build_smoke_test_result.py || echo -e "failure\tCannot parse results" > $WORKPATH/test_output/check_status.tsv

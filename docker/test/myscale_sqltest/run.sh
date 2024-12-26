#!/bin/bash
# shellcheck disable=SC2015

set -x
set -e
set -u
set -o pipefail

TEST_OUTPUT=$WORKPATH/test_output
arch="amd64" #hardcoded for now

# install clickhouse deb from artifacts
function install_clickhouse_deb
{
    dpkg -i $WORKPATH/packages/clickhouse-common-static_*$arch.deb
    dpkg -i $WORKPATH/packages/clickhouse-common-static-dbg_*$arch.deb
    dpkg -i $WORKPATH/packages/clickhouse-server_*$arch.deb
    dpkg -i $WORKPATH/packages/clickhouse-client_*$arch.deb
}

install_clickhouse_deb

echo "
users:
  default:
    access_management: 1" > /etc/clickhouse-server/users.d/access_management.yaml

echo '<clickhouse><listen_host>0.0.0.0</listen_host></clickhouse>' >/etc/clickhouse-server/config.d/listen.xml
echo '<clickhouse><interserver_listen_host>0.0.0.0</interserver_listen_host></clickhouse>' >/etc/clickhouse-server/config.d/interserver_listen_host.xml


clickhouse start

# Wait for start
for _ in {1..100}
do
    clickhouse-client --query "SELECT 1" && break ||:
    sleep 1
done

# Run the test
pushd /sqltest/standards/2016/
/test.py
mv report.html test.log $TEST_OUTPUT
popd

zstd --threads=0 /var/log/clickhouse-server/clickhouse-server.log
zstd --threads=0 /var/log/clickhouse-server/clickhouse-server.err.log

mv /var/log/clickhouse-server/clickhouse-server.log.zst /var/log/clickhouse-server/clickhouse-server.err.log.zst $TEST_OUTPUT

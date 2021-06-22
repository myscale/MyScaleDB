#!/bin/bash

# script allows to install configs for clickhouse server and clients required
# for testing (stateless and stateful tests)

set -x -e

DEST_SERVER_PATH="${1:-/etc/clickhouse-server}"
DEST_CLIENT_PATH="${2:-/etc/clickhouse-client}"
SRC_PATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

echo "Going to install test configs from $SRC_PATH into $DEST_SERVER_PATH"

mkdir -p $DEST_SERVER_PATH/config.d/
mkdir -p $DEST_SERVER_PATH/users.d/
mkdir -p $DEST_CLIENT_PATH


if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
    ln -sf $SRC_PATH/users.d/database_replicated.xml $DEST_SERVER_PATH/users.d/
    ln -sf $SRC_PATH/config.d/database_replicated.xml $DEST_SERVER_PATH/config.d/
    rm /etc/clickhouse-server/config.d/zookeeper.xml
    rm /etc/clickhouse-server/config.d/keeper_port.xml

    # There is a bug in config reloading, so we cannot override macros using --macros.replica r2
    # And we have to copy configs...
    mkdir -p /etc/clickhouse-server1
    mkdir -p /etc/clickhouse-server2
    cp -r /etc/clickhouse-server/* /etc/clickhouse-server1
    clickhouse cp -r /etc/clickhouse-server/* /etc/clickhouse-server2
    rm /etc/clickhouse-server1/config.d/macros.xml
    rm /etc/clickhouse-server2/config.d/macros.xml
    sudo -u clickhouse cat /etc/clickhouse-server/config.d/macros.xml | sed "s|<replica>r1</replica>|<replica>r2</replica>|" > /etc/clickhouse-server1/config.d/macros.xml
    sudo -u clickhouse cat /etc/clickhouse-server/config.d/macros.xml | sed "s|<shard>s1</shard>|<shard>s2</shard>|" > /etc/clickhouse-server2/config.d/macros.xml

    sudo mkdir -p /var/lib/clickhouse1
    sudo mkdir -p /var/lib/clickhouse2
    sudo chown clickhouse /var/lib/clickhouse1
    sudo chown clickhouse /var/lib/clickhouse2
    sudo chgrp clickhouse /var/lib/clickhouse1
    sudo chgrp clickhouse /var/lib/clickhouse2
fi
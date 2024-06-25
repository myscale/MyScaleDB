#!/usr/bin/env bash
set -e -x

TZ='Asia/Shanghai'

if [ $# -ne 2 ]; then
    echo "Usage: $0 ck-version myscale-version"
    exit 1
fi

CK_VERSION=${1:-23.3.2.1}
MYSCALE_VERSION=${2:-1.6.0}

function init_ec2-user() {
    # init user: ec2-user
    adduser --disabled-password --gecos "" ec2-user
    chown -R ec2-user:ec2-user /etc/clickhouse-server
    chown -R ec2-user:ec2-user /etc/clickhouse-client
    chown -R ec2-user:ec2-user /etc/clickhouse-keeper
    chown -R ec2-user:ec2-user /var/lib/clickhouse
    chown -R ec2-user:ec2-user /var/log/clickhouse-server
    chown -R ec2-user:ec2-user /var/log/clickhouse-keeper
    chown -R ec2-user:ec2-user /var/log/clickhouse-client
    chown -R ec2-user:ec2-user /var/run/clickhouse-server
    chown -R ec2-user:ec2-user /var/run/clickhouse-keeper
}

function unzip_upload_file() {
    # decompression upload.zip
    unzip /tmp/upload.zip -d /tmp/upload
    tree /tmp
    export upload_unzip_folder='/tmp/upload'
}

function install_clickhouse_tgz() {
    package_folder='/tmp/package'
    mkdir -p ${package_folder}

    # decompression clickhouse package
    tar -xvf ${upload_unzip_folder}/clickhouse-$CK_VERSION-amd64.tgz -C ${package_folder}
    myscale_usr_folder="${package_folder}/clickhouse-$CK_VERSION-amd64/usr"

    # install clickhouse server, client, keeper
    mv ${myscale_usr_folder}/bin/* /usr/bin/.
}

function install_clickhouse_conf() {
    myscale_conf="${upload_unzip_folder}/myscale"
    mkdir -p /etc/clickhouse-server
    mkdir -p /etc/clickhouse-client
    mkdir -p /etc/clickhouse-keeper
    mv ${myscale_conf}/server.conf/* /etc/clickhouse-server/.
    mv ${myscale_conf}/client.conf/* /etc/clickhouse-client/.
    mv ${myscale_conf}/keeper.conf/* /etc/clickhouse-keeper/.
    # create clickhouse related folders
    mkdir -p /var/lib/clickhouse
    mkdir -p /var/log/clickhouse-server
    mkdir -p /var/log/clickhouse-keeper
    mkdir -p /var/log/clickhouse-client
    mkdir -p /var/run/clickhouse-server
    mkdir -p /var/run/clickhouse-keeper
}

function disable-root-ssh() {
    # Define the SSH configuration file path
    SSHD_CONFIG="/etc/ssh/sshd_config"

    # Check if PermitRootLogin is already set in the configuration file
    if grep -q "^PermitRootLogin" "$SSHD_CONFIG"; then
        # If the setting exists, change its value to 'no'
        sed -i "s/^PermitRootLogin.*/PermitRootLogin no/" "$SSHD_CONFIG"
    else
        # If the setting does not exist, add it to the end of the file
        echo "PermitRootLogin no" | tee -a "$SSHD_CONFIG"
    fi
}

unzip_upload_file

install_clickhouse_tgz

install_clickhouse_conf

init_ec2-user

disable-root-ssh

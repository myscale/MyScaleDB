#!/usr/bin/env bash
set -e

current_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
package_folder="${current_dir}/../package"
upload_packer_folder="${current_dir}/../upload"
myscale_conf_folder="${current_dir}/../../../docker/myscale"
myscale_other_config_conf="${current_dir}/../myscale.conf/myscale_ami_config.xml"
myscale_other_users_conf="${current_dir}/../myscale.conf/myscale_ami_users.xml"

rm -rf ${upload_packer_folder} || true
mkdir -p ${upload_packer_folder}

# check clickhouse-*.tgz is exist
if ! ls "${package_folder}"/clickhouse-* 1> /dev/null 2>&1; then
    echo "clickhouse-* not found in ${package_folder}"
    exit 1
fi

cp -r ${package_folder}/clickhouse-*.tgz ${upload_packer_folder}/
cp -r ${myscale_conf_folder} ${upload_packer_folder}/.
cp ${myscale_other_config_conf} ${upload_packer_folder}/myscale/server.conf/config.d/
cp ${myscale_other_users_conf} ${upload_packer_folder}/myscale/server.conf/users.d/
cd ${upload_packer_folder}
tree .
zip -q -r ${current_dir}/../upload.zip ./*
cd ${current_dir}
rm -rf ${upload_packer_folder}
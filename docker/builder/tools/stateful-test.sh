#!/usr/bin/env bash
set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_PATH=$CUR_DIR/../../..
WORKPATH=$PROJECT_PATH/docker/test/myscale_run_stateful

cp -rfv artifacts/clickhouse-*.deb docker/test/myscale_run_stateful/packages
cp -rfv docker/test/myscale_test_script/s3downloader docker/test/myscale_run_stateful/s3downloader
cp -rfv tests/queries docker/test/myscale_run_stateful/tests/
cp -rfv tests/performance docker/test/myscale_run_stateful/tests/
cp -rfv tests/config docker/test/myscale_run_stateful/tests/
cp -rfv tests/clickhouse-test docker/test/myscale_run_stateful/
cp -rfv docker/test/myscale_run_stateful/create.sql /create.sql

ln -snf $WORKPATH/packages /package_folder
ln -snf $WORKPATH/clickhouse-test /usr/bin/clickhouse-test
ln -snf $WORKPATH/s3downloader /s3downloader
ln -snf $WORKPATH/tests /usr/share/clickhouse-test
ln -snf $WORKPATH/test_output /test_output

cd /

export USE_AZURE_STORAGE_FOR_MERGE_TREE=0;
MAX_RUN_TIME=9720 ADDITIONAL_OPTIONS="--hung-check --print-time --no-vector-search" \
  DATASETS_URL="https://mqdb-release-1253802058.cos.ap-beijing.myqcloud.com/datasets" \
  DATASETS="hits visits" \
  /bin/bash $WORKPATH/run.sh

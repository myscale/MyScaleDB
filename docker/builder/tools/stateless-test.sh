#!/usr/bin/env bash
set -e

source /etc/profile
arch="$(dpkg --print-architecture)"
if [[ "x$arch" = "xamd64" ]]; then
    FORCE_RETRY="--force-retry"
fi

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_PATH=$CUR_DIR/../../..
WORKPATH=$PROJECT_PATH/docker/test/mqdb_run_stateless

cp -rfv artifacts/clickhouse-*.deb docker/test/mqdb_run_stateless/packages
cp -rfv tests/queries docker/test/mqdb_run_stateless/tests/
cp -rfv tests/performance docker/test/mqdb_run_stateless/tests/
cp -rfv tests/config docker/test/mqdb_run_stateless/tests/
cp -rfv tests/clickhouse-test docker/test/mqdb_run_stateless/

ln -snf $WORKPATH/packages /package_folder
ln -snf $WORKPATH/clickhouse-test /usr/bin/clickhouse-test
ln -snf $WORKPATH/tests /usr/share/clickhouse-test
ln -snf $WORKPATH/test_output /test_output

cd /

MAX_RUN_TIME=9720 S3_URL="https://clickhouse-datasets.s3.amazonaws.com" \
  ADDITIONAL_OPTIONS="--hung-check --print-time --no-vector-search $FORCE_RETRY" \
  /bin/bash $WORKPATH/run.sh

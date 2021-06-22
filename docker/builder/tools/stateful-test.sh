#!/usr/bin/env bash
set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_PATH=$CUR_DIR/../../..
WORKPATH=$PROJECT_PATH/docker/test/mqdb_run_stateful

cp -rfv artifacts/clickhouse-*.deb docker/test/mqdb_run_stateful/packages
cp -rfv docker/test/mqdb_test_script/s3downloader docker/test/mqdb_run_stateful/s3downloader
cp -rfv tests/queries docker/test/mqdb_run_stateful/tests/
cp -rfv tests/performance docker/test/mqdb_run_stateful/tests/
cp -rfv tests/config docker/test/mqdb_run_stateful/tests/
cp -rfv tests/clickhouse-test docker/test/mqdb_run_stateful/

docker rmi -f run-stateful-test >/dev/null 2>&1 || true
docker build --rm=true -t run-stateful-test docker/test/mqdb_run_stateful

docker run --rm --user root --volume=$WORKPATH/test_output:/test_output --cap-add=SYS_PTRACE -e MAX_RUN_TIME=9720 -e ADDITIONAL_OPTIONS="--hung-check --print-time --no-vector-search" --name stateful-test run-stateful-test

docker rmi -f run-stateful-test >/dev/null 2>&1 || true
docker rm -f stateful-test >/dev/null 2>&1 || true

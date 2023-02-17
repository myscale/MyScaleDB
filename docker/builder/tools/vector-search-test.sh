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

THREAD_SANITIZER_TEST=${1:-false}
if [ $THREAD_SANITIZER_TEST == "true" ]; then
    sed -i 's?FROM harbor.internal.moqi.ai/mqdb/mqdb-test-stateless:1.2?FROM harbor.internal.moqi.ai/mqdb/mqdb-test-stateless:1.3?' docker/test/mqdb_run_stateless/Dockerfile
fi

mkdir docker/test/mqdb_run_stateless/tests/queries
cp -rfv artifacts/clickhouse-*.deb docker/test/mqdb_run_stateless/packages
cp -rfv docker/test/mqdb_test_script/clickhouse-test docker/test/mqdb_run_stateless/clickhouse-test
cp -rfv tests/queries/2_vector_search docker/test/mqdb_run_stateless/tests/queries/
cp -rfv tests/performance docker/test/mqdb_run_stateless/tests/
cp -rfv tests/config docker/test/mqdb_run_stateless/tests/
cp -rfv tests/clickhouse-test docker/test/mqdb_run_stateless/

docker rm -f stateless-test >/dev/null 2>&1 || true
docker build --rm=true -t run-stateless-test docker/test/mqdb_run_stateless

docker run --rm --user root --volume=$WORKPATH/test_output:/test_output --cap-add=SYS_PTRACE -e MAX_RUN_TIME=9720 -e S3_URL="https://clickhouse-datasets.s3.amazonaws.com" -e ADDITIONAL_OPTIONS="--hung-check --print-time --no-stateless --no-stateful $FORCE_RETRY" --name stateless-test run-stateless-test

docker rm -f stateless-test >/dev/null 2>&1 || true
docker rmi -f run-stateless-test >/dev/null 2>&1 || true
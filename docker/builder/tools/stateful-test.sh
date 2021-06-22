#!/usr/bin/env bash
set -e

cp -rfv artifacts/clickhouse-*.deb docker/test/mqdb_run_stateful/packages
cp -rfv tests/queries docker/test/mqdb_run_stateful/tests/
cp -rfv tests/performance docker/test/mqdb_run_stateful/tests/
cp -rfv tests/config docker/test/mqdb_run_stateful/tests/
cp -rfv tests/clickhouse-test docker/test/mqdb_run_stateful/

docker rmi -f run-stateful-test >/dev/null 2>&1 || true
docker build --rm=true -t run-stateful-test docker/test/mqdb_run_stateful

docker run --rm --user root --cap-add=SYS_PTRACE -e MAX_RUN_TIME=9720 -e ADDITIONAL_OPTIONS="--hung-check --print-time --no-vector-search" --name stateful-test run-stateful-test

docker rmi -f run-stateful-test >/dev/null 2>&1 || true
docker rm -f stateful-test >/dev/null 2>&1 || true

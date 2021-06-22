#!/usr/bin/env bash
set -e
host=${1:-127.0.0.1}
time_out=${2:-259200} # 3d
parallel=${3:-4} # 4 个并发
new_sql=${4:-1}
#host=chi-testing-testing-0-0-0.chi-testing-testing-0-0.mqdb-single-node.svc.cluster.local

# source /etc/profile
arch="$(dpkg --print-architecture)"
if [[ "x$arch" = "xamd64" ]]; then
    FORCE_RETRY="--force-retry"
fi

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_PATH=$CUR_DIR/../../..


docker rm -f benchmark-test >/dev/null 2>&1 || true
docker rmi -f run-benchmark-test >/dev/null 2>&1 || true
docker build --rm=true -t run-benchmark-test $PROJECT_PATH/docker/test/mqdb_run_stability/benchmark

docker run -itd \
    --user root --cap-add=SYS_PTRACE \
    -e PARALLEL=${parallel} -e TIME_OUT=${time_out} -e HOST=${host} -e NEW_SQL=${new_sql} \
    --name benchmark-test run-benchmark-test
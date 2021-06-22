#!/usr/bin/env bash
set -e
HOST=${1:-127.0.0.1}
TIMEOUT=${2:-259200} # 3d
INDEX_TYPE=${3:-"MSTG"}

# source /etc/profile
arch="$(dpkg --print-architecture)"
if [[ "x$arch" = "xamd64" ]]; then
    FORCE_RETRY="--force-retry"
fi

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_PATH=$CUR_DIR/../../..
WORKPATH=$PROJECT_PATH/docker/test/mqdb_run_stability
rm -rf $WORKPATH/test_output/* ||:


docker rm -f lwd-test >/dev/null 2>&1 || true
docker rmi -f run-lwd-test >/dev/null 2>&1 || true
docker build --rm=true -t run-lwd-test $PROJECT_PATH/docker/test/mqdb_run_stability/lwd

docker run -itd \
    --user root --cap-add=SYS_PTRACE \
    -e HOST=${HOST} -e TIMEOUT=${TIMEOUT} -e INDEX_TYPE=${INDEX_TYPE} \
    --volume=$WORKPATH/test_output:/home/minimum_test/lwd_feature_test/results \
    --name lwd-test run-lwd-test
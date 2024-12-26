#!/usr/bin/env bash
set -e

DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $DIR/init-env.sh myscale_performance
SHA_TO_TEST=${2:-run_for_test}

echo "***set env***"
export PR_TO_TEST=0
export CHPC_TEST_RUN_BY_HASH_TOTAL=3 
export CHPC_TEST_RUN_BY_HASH_NUM=${3:-0}

function clean
{
    # For local run, remove last test file
    echo "***remove last test file***"
    rm -rf $WORKPATH/tests/* ||:;
    rm -rf $WORKPATH/packages/* ||:;
    rm -rf $WORKPATH/test_output/* ||:;
    rm -rf $WORKPATH/workspace ||:;
    tree -L 2 $WORKPATH
}
function run
{
    node=$(($RANDOM % $(numactl --hardware | sed -n 's/^.*available:\(.*\)nodes.*$/\1/p')));
    echo Will bind to NUMA node $node; 
    numactl --cpunodebind=$node --membind=$node $WORKPATH/entrypoint.sh $PROJECT_PATH $SHA_TO_TEST $1
}

if [[ $1 == "clean" ]];
then
    clean
    exit 0;
elif [[ $1 == "skip_copy" ]];
then
    echo "***RUN TEST***";
    run "skip_copy"
    exit 0;
fi

echo "***Copy the file to the relevant directory***"
clean

# In ci test, all CK installation packages are from 
# ${PROJECT_PATH}/artifacts path. we can copy the file that we want
cp -rfv ${PROJECT_PATH}/artifacts/performance_pack_amd64* $WORKPATH/tests/performance_pack_right.tar.gz;
# 梳理下载最新的 mqdb-dev 分支最新的
# wget -P $WORKPATH/tests/ https://git.moqi.ai/mqdb/ClickHouse/-/jobs/artifacts/mqdb_dev/download?job=build_binary

echo "***Test environment initialization completed***"
tree -L 2 $WORKPATH

echo "***RUN TEST***"
run
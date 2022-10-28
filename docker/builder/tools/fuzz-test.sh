#!/usr/bin/env bash
set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_PATH=$CUR_DIR/../../..
WORKPATH=$PROJECT_PATH/docker/test/mqdb_run_fuzzer
SHA_TO_TEST=${2:-run_for_test}

function clean
{
    # For local run, remove last test file
    echo "***remove last test file***"
    rm -rf $WORKPATH/tests/* ||:;
    rm -rf $WORKPATH/packages/* ||:;
    rm -rf $WORKPATH/test_output/* ||:;
    rm -rf $WORKPATH/ch $WORKPATH/db $WORKPATH/workspace ||:;
    tree -L 2 $WORKPATH
}

if [[ $1 == "clean" ]];
then
    clean
    exit 0;
elif [[ $1 == "skip_copy" ]];
then
    echo "***RUN TEST***";
    bash $WORKPATH/run-fuzzer.sh $PROJECT_PATH $SHA_TO_TEST;
    exit 0;
fi

echo "***Copy the file to the relevant directory***"
clean
# In ci test, all CK installation packages are from 
# ${PROJECT_PATH}/artifacts path. we can copy the file that we want
cp -rfv ${PROJECT_PATH}/artifacts/clickhouse-*.deb $WORKPATH/packages/.;
cp -rfL ${PROJECT_PATH}/programs/server $WORKPATH/tests/.
rsync -a --exclude='integration/*' ${PROJECT_PATH}/tests $WORKPATH/tests/.

echo "***Test environment initialization completed***"
tree -L 2 $WORKPATH

echo "***RUN TEST***"
bash $WORKPATH/run-fuzzer.sh $PROJECT_PATH $SHA_TO_TEST
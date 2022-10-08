#!/usr/bin/env bash
set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_PATH=$CUR_DIR/../../..
WORKPATH=$PROJECT_PATH/docker/test/mqdb_run_smoke
SHA_TO_TEST=${2:-run_for_test}

if [[ $1 == "clean" ]];
then
    rm -rf $PROJECT_PATH/docker/test/mqdb_test_stress/tests/*;
    rm -rf $PROJECT_PATH/docker/test/mqdb_test_stress/packages/*;
    rm -rf $PROJECT_PATH/docker/test/mqdb_test_stress/test_output/*;
    exit 0;
fi

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

if [[ $1 == "clean" ]];
then
    clean
    exit 0;
elif [[ $1 == "skip_copy" ]];
then
    echo "***RUN TEST***";
    bash $WORKPATH/run.sh $PROJECT_PATH $SHA_TO_TEST;
    exit 0;
fi

echo "***Copy the file to the relevant directory***"
clean

# In ci test, all CK installation packages are from 
# ${PROJECT_PATH}/artifacts path. we can copy the file that we want
cp -rfv ${PROJECT_PATH}/artifacts/clickhouse-*.deb $WORKPATH/packages/.;

echo "***Test environment initialization completed***"
tree -L 2 $WORKPATH

echo "***RUN TEST***"
bash $WORKPATH/run.sh $PROJECT_PATH $SHA_TO_TEST
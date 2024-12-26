#!/usr/bin/env bash
set -e

DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $DIR/init-env.sh myscale_fuzzer
SHA_TO_TEST=${CI_COMMIT_SHA:-run_for_test}

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

function run_test
{
    echo "***RUN TEST***";
    source $WORKPATH/run-fuzzer.sh
}

function copy_file
{
    echo "***Copy the file to the relevant directory***"
    clean
    cp -rf ${PROJECT_PATH}/artifacts/clickhouse-*.deb $WORKPATH/packages/.;
    cp -rfL ${PROJECT_PATH}/programs/server $WORKPATH/tests/.
    rsync -a --exclude='integration/*' ${PROJECT_PATH}/tests $WORKPATH/tests/.

    echo "***Test environment initialization completed***"
    tree -L 2 $WORKPATH
}

if [[ $1 == "clean" ]];
then
    clean
elif [[ $1 == "skip_copy" ]];
then
    run_test
else
    copy_file
    run_test
fi
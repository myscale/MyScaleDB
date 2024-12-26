#!/usr/bin/env bash

DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $DIR/init-env.sh myscale_stress
SHA_TO_TEST=${CI_COMMIT_SHA:-run_for_test}

function clean
{
    # For local run, remove last test file
    echo "***remove last test file***"
    rm -rf $WORKPATH/tests/* ||:;
    rm -rf $WORKPATH/packages/* ||:;
    rm -rf $WORKPATH/test_output/* ||:;
    rm -rf $WORKPATH/workspace ||:;
    rm -rf rm -rf /etc/clickhouse-server/* ||:
    rm -rf rm -rf /var/lib/clickhouse/* ||:
    tree -L 2 $WORKPATH
}

function copy_file
{
    echo "***Copy the file to the relevant directory***"
    clean
    cp -rf ${PROJECT_PATH}/artifacts/clickhouse-*.deb $WORKPATH/packages/.;
    cp -rf ${PROJECT_PATH}/tests/clickhouse-test $WORKPATH/tests/
    cp -rf ${PROJECT_PATH}/tests/config $WORKPATH/tests/
    cp -rf ${PROJECT_PATH}/tests/queries $WORKPATH/tests/
    rm -rf ${WORKPATH}/tests/queries/2_vector_search $WORKPATH/tests/queries/3_ai_core_support ||:
    cp -rf ${PROJECT_PATH}/tests/performance $WORKPATH/tests/
    cp -rf ${PROJECT_PATH}/tests/ci $WORKPATH/tests/
    # cp -rf ${PROJECT_PATH}/tests/clickhouse-test $WORKPATH/tests/
    echo "***Test environment initialization completed***"
    tree -L 2 $WORKPATH
}

function run_test
{
    cd /
    echo "***RUN TEST***"
    export USE_AZURE_STORAGE_FOR_MERGE_TREE=0;
    export USE_OLD_ANALYZER=0;
    source $WORKPATH/run.sh
}

if [[ $1 == "clean" ]];
then
    clean
    exit 0;
elif [[ $1 == "skip_copy" ]];
then
    run_test
else    
    copy_file
    run_test
fi

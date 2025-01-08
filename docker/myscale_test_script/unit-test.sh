#!/usr/bin/env bash
set -e

DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $DIR/init-env.sh myscale_unit

function clean
{
    # For local run, remove last test file
    echo "***remove last test file***"
    rm -rf $WORKPATH/unit_tests_dbms ||:;
    rm -rf $WORKPATH/test_output/* ||:;
    tree -L 2 $WORKPATH
}

function copy_file
{
    echo "***Copy the file to the relevant directory***"
    clean
    cp -rfv $PROJECT_PATH/artifacts/unit_tests_dbms $WORKPATH/.
    echo "***Test environment initialization completed***"
    tree -L 2 $WORKPATH
}

function run_test
{
    echo "***RUN TEST***"
    cd $WORKPATH
    source $WORKPATH/run.sh NO_GDB
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

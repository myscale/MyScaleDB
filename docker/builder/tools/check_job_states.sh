#!/usr/bin/env bash
set -e

TEST_NAME=${1:-NONE}
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_PATH=$CUR_DIR/../../..
WORKPATH=$PROJECT_PATH/docker/test/$TEST_NAME/test_output

function check_stateless
{
    TESTS_STATE=$(cat $WORKPATH/check_status.tsv | awk '{print $1}')
    cat $WORKPATH/check_status.tsv
    if [[ "$TESTS_STATE" == "failure" ]]; then
        echo "test error, please check the test log"
        exit 1
    else
        echo "test pass"
    fi
}

function check_stateful
{
    TESTS_STATE=$(cat $WORKPATH/check_status.tsv | awk '{print $1}')
    cat $WORKPATH/check_status.tsv
    if [[ "$TESTS_STATE" == "failure" ]]; then
        echo "test error, please check the test log"
        exit 1
    else
        echo "test pass"
    fi
}

function check_fuzzer
{
    TEST_STATE=$(cat $WORKPATH/report.html | grep '<tr><td>AST Fuzzer</td><td>' | grep 'success' | wc -l)
    if [[ "$TESTS_STATE" == "0" ]]; then
        echo "test error, please check the test log"
        exit 1
    else
        echo "test pass"
    fi
}

function check_smoke
{
    TESTS_STATE=$(cat $WORKPATH/check_status.tsv | awk '{print $1}')
    cat $WORKPATH/check_status.tsv
    if [[ "$TESTS_STATE" != "success" ]]; then
        echo "test error, please check the test log"
        exit 1
    else
        echo "test pass"
    fi
}

function check_stress
{
    TESTS_STATE=$(cat $WORKPATH/check_status.tsv | awk '{print $1}')
    cat $WORKPATH/check_status.tsv
    if [[ "$TESTS_STATE" == "failure" ]]; then
        echo "test error, please check the test log"
        exit 1
    else
        echo "test pass"
    fi
}

function check_performance
{
    echo "don't support"
}

function check_integration
{
    echo "don't support"
}

if [[ "$TEST_NAME" == "mqdb_run_stateful" ]]; then
    check_stateful
elif [[ "$TEST_NAME" == "mqdb_run_stateless" ]]; then
    check_stateless
elif [[ "$TEST_NAME" == "mqdb_run_fuzzer" ]]; then
    check_fuzzer
elif [[ "$TEST_NAME" == "mqdb_run_smoke" ]]; then
    check_smoke
elif [[ "$TEST_NAME" == "mqdb_run_stress" ]]; then
    check_stress
elif [[ "$TEST_NAME" == "mqdb_run_performance" ]]; then
    check_performance
elif [[ "$TEST_NAME" == "mqdb_run_integration" ]]; then
    check_integration
else
    echo "please entry the correct test name"
fi
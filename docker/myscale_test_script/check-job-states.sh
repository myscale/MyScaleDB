#!/usr/bin/env bash
set -e

TEST_NAME=${1:-NONE}
WITH_SANITIZER=${2}
DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $DIR/init-env.sh $TEST_NAME/test_output

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

# It only detects sanitizer-related problems and is a temporary solution
function check_stress_temporary
{
    TEST_STATE=$(cat $WORKPATH/test_results.tsv | grep sanitizer | awk '{x=$(NF-1)}END{print x}')
    cat $WORKPATH/test_results.tsv
    if [[ "$TEST_STATE" != "OK" ]]; then
        echo "sanitizer check error, please check the test log"
        exit 1
    else
        echo "test pass"
    fi
}

function check_stateles_sanitizer_test
{
    TESTS_STATE=$(cat $WORKPATH/check_status.tsv | awk '{print $1}')
    IS_FINISHED=$(cat $WORKPATH/check_status.tsv | awk '{print $4}')
    if [ "$TESTS_STATE" == "failure" ] && [ "$IS_FINISHED" == "not" ]; then
        echo "sanitizer check error, please check the test log"
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

function check_unit
{
    echo "don't support"
}

if [[ "$TEST_NAME" == "myscale_stateful" ]]; then
    check_stateful
elif [[ "$TEST_NAME" == "myscale_stateless" ]]; then
    if [ -n "$WITH_SANITIZER" ]; then
        check_stateles_sanitizer_test
    else
        check_stateless
    fi
elif [[ "$TEST_NAME" == "myscale_fuzzer" ]]; then
    check_fuzzer
elif [[ "$TEST_NAME" == "myscale_stress" ]]; then
    # check_stress
    check_stress_temporary
elif [[ "$TEST_NAME" == "myscale_performance" ]]; then
    check_performance
elif [[ "$TEST_NAME" == "myscale_integration" ]]; then
    check_integration
elif [[ "$TEST_NAME" == "myscale_unit" ]]; then
    check_unit
else
    echo "please entry the correct test name"
fi
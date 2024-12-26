#!/usr/bin/env bash

if [[ -z $1 ]]; then
    echo "Please input the test name"
    exit 1
fi

DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $DIR/init-env.sh DUMMY

TEST_NAME=$1
CLEAN_CMD=$2

if [[ $TEST_NAME == 'vector-test' ]];
then
    TEST_IMAGE=harbor.internal.moqi.ai/mqdb/mqdb-test-stateless:3.0.0
    SCRIPTS="docker/myscale_test_script/stateless-test.sh $CLEAN_CMD true"
elif [[ $TEST_NAME == 'stateless-test' ]];
then
    TEST_IMAGE=harbor.internal.moqi.ai/mqdb/mqdb-test-stateless:3.0.0
    SCRIPTS="docker/myscale_test_script/stateless-test.sh $CLEAN_CMD"
elif [[ $TEST_NAME == 'stateful-test' ]];
then
    TEST_IMAGE=harbor.internal.moqi.ai/mqdb/mqdb-test-stateless:3.0.0
    SCRIPTS="docker/myscale_test_script/stateful-test.sh $CLEAN_CMD"
elif [[ $TEST_NAME == 'fuzz-test' ]];
then
    TEST_IMAGE=harbor.internal.moqi.ai/mqdb/mqdb-test-fuzzer:3.0.0
    SCRIPTS="docker/myscale_test_script/fuzz-test.sh $CLEAN_CMD"
elif [[ $TEST_NAME == 'stress-test' ]];
then
    TEST_IMAGE=harbor.internal.moqi.ai/mqdb/mqdb-test-stress:3.0.0
    SCRIPTS="docker/myscale_test_script/stress-test.sh $CLEAN_CMD"
elif [[ $TEST_NAME == 'sqlancer-test' ]];
then
    TEST_IMAGE=harbor.internal.moqi.ai/mqdb/mqdb-test-sqlancer:3.0.0
    SCRIPTS="docker/myscale_test_script/sqlancer-test.sh $CLEAN_CMD"
elif [[ $TEST_NAME == 'sqltest-test' ]];
then
    TEST_IMAGE=harbor.internal.moqi.ai/mqdb/mqdb-test-sqltest:3.0.0
    SCRIPTS="docker/myscale_test_script/sqltest-test.sh $CLEAN_CMD"
else
    echo "Please input the correct test name"
    exit 1
fi

echo "will run: docker run --privileged=true --rm --name $TEST_NAME \
    --volume=$PROJECT_PATH:/workspace/mqdb \
    -w /workspace/mqdb \
    $TEST_IMAGE $SCRIPTS"

docker run --privileged=true --rm --name $TEST_NAME \
    --volume=$PROJECT_PATH:/workspace/mqdb \
    -w /workspace/mqdb \
    $TEST_IMAGE $SCRIPTS

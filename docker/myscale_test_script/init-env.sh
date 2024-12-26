#!/usr/bin/env bash

if [[ -z $1 ]]; then
    echo "Please input the test name"
    exit 1
fi

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_PATH=$CUR_DIR/../..
WORKPATH=$PROJECT_PATH/docker/test/$1
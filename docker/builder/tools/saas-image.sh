#!/usr/bin/env bash
set -e

if [ $# -ne 2 ]; then
    echo 'Requires docker hub user account and password!'
    exit 1
fi

source docker/builder/tools/version.sh

cp -rfv artifacts/clickhouse-*.tgz docker/mqdb/

DOCKER_HUB_USER_NAME="$1"
DOCKER_HUB_USER_PASSWORD="$2"

echo $DOCKER_HUB_USER_PASSWORD | docker login --username $DOCKER_HUB_USER_NAME --password-stdin

docker buildx build --build-arg 'http_proxy=http://clash.internal.moqi.ai:7890' --build-arg 'https_proxy=http://clash.internal.moqi.ai:7890' --build-arg 'no_proxy=localhost,127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,docker,git.moqi.ai,.internal.moqi.ai' --platform linux/amd64,linux/arm64 --build-arg version="${VERSION_STRING}" --rm=true -t $DOCKER_HUB_USER_NAME/mqdb:${VERSION_STRING}-${GIT_COMMIT} docker/mqdb --push

rm -rfv docker/mqdb/clickhouse-*.tgz

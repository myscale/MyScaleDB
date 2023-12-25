#!/usr/bin/env bash
set -e

if [ $# -ne 2 ]; then
    echo 'Requires harbor user account and password!'
    exit 1
fi

source docker/builder/tools/version.sh

cp -rfv artifacts/clickhouse-*.tgz docker/myscale/

HARBOR_USER_NAME="$1"
HARBOR_USER_PASSWORD="$2"
HARBOR_REGISTRY="harbor.internal.moqi.ai"
HARBOR_NAMESPACE="mqdb"

echo $HARBOR_USER_PASSWORD | docker login $HARBOR_REGISTRY --username $HARBOR_USER_NAME --password-stdin

docker buildx build --build-arg 'http_proxy=http://clash.internal.moqi.ai:7890' --build-arg 'https_proxy=http://clash.internal.moqi.ai:7890' --build-arg 'no_proxy=localhost,127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,docker,git.moqi.ai,.internal.moqi.ai' --platform linux/amd64 --build-arg version="${MYSCALE_VERSION_MAJOR}.${MYSCALE_VERSION_MINOR}" --rm=true -t $HARBOR_REGISTRY/$HARBOR_NAMESPACE/myscaledb:${MYSCALE_VERSION_MAJOR}.${MYSCALE_VERSION_MINOR} docker/myscale --push

rm -rfv docker/myscale/clickhouse-*.tgz

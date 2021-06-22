#!/usr/bin/env bash
set -e

source docker/builder/tools/version.sh

cp -rfv artifacts/clickhouse-*.tgz docker/mqdb/

HARBOR_USER_NAME="yim@moqi.ai"
HARBOR_USER_PASSWORD="Zhu88jie"
HARBOR_REGISTRY="harbor.internal.moqi.ai"
HARBOR_NAMESPACE="mqdb"

echo $HARBOR_USER_PASSWORD | docker login $HARBOR_REGISTRY --username $HARBOR_USER_NAME --password-stdin

docker buildx build --build-arg 'http_proxy=http://clash.internal.moqi.ai:7890' --build-arg 'https_proxy=http://clash.internal.moqi.ai:7890' --build-arg 'no_proxy=localhost,127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,docker,git.moqi.ai,.internal.moqi.ai' --platform linux/amd64,linux/arm64 --build-arg version="${VERSION_STRING}" --rm=true -t $HARBOR_REGISTRY/$HARBOR_NAMESPACE/mqdb:${VERSION_STRING}-${GIT_COMMIT}-with-license docker/mqdb --push

rm -rfv docker/mqdb/clickhouse-*.tgz

#!/usr/bin/env bash
set -e

source docker/builder/tools/version.sh

cp -rfv artifacts/clickhouse-*.tgz docker/mqdb/
cp -rfv artifacts/clickhouse-*.tgz docker/mqdb-client/
cp -rfv artifacts/clickhouse-*.tgz docker/mqdb-ssh-client/
cp -fv tests/vector_search/benchmark.py docker/mqdb-ssh-client/

HARBOR_USER_NAME="yim@moqi.ai"
HARBOR_USER_PASSWORD="Zhu88jie"
HARBOR_REGISTRY="harbor.internal.moqi.ai"
HARBOR_NAMESPACE="mqdb"

echo $HARBOR_USER_PASSWORD | docker login $HARBOR_REGISTRY --username $HARBOR_USER_NAME --password-stdin

docker buildx build --platform linux/amd64,linux/arm64 --build-arg version="${VERSION_STRING}" --rm=true -t $HARBOR_REGISTRY/$HARBOR_NAMESPACE/mqdb:${VERSION_STRING}-${GIT_COMMIT} docker/mqdb --push

docker buildx build --platform linux/amd64,linux/arm64 --build-arg version="${VERSION_STRING}" --rm=true -t $HARBOR_REGISTRY/$HARBOR_NAMESPACE/mqdb-client:${VERSION_STRING}-${GIT_COMMIT} docker/mqdb-client --push

docker buildx build --platform linux/amd64,linux/arm64 --build-arg version="${VERSION_STRING}" --rm=true -t $HARBOR_REGISTRY/$HARBOR_NAMESPACE/mqdb-ssh-client:${VERSION_STRING}-${GIT_COMMIT} docker/mqdb-ssh-client --push

rm -rfv docker/mqdb/clickhouse-*.tgz
rm -rfv docker/mqdb-client/clickhouse-*.tgz
rm -rfv docker/mqdb-ssh-client/clickhouse-*.tgz

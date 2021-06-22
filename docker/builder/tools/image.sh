#!/usr/bin/env bash
set -e

source docker/builder/tools/version.sh

cp -rfv artifacts/clickhouse-*.tgz docker/mqdb/
cp -rfv artifacts/clickhouse-*.tgz docker/mqdb-client/
cp -rfv artifacts/clickhouse-*.tgz docker/mqdb-ssh-client/
cp -fv tests/vector_search/benchmark.py docker/mqdb-ssh-client/

HARBOR_USER_NAMENAME="yim@moqi.ai"
HARBOR_USER_PASSWORD="Zhu88jie"
REGISTRY="harbor.internal.moqi.ai"
NAMESPACE="mqdb"

echo $HARBOR_USER_PASSWORD | docker login $REGISTRY --username $HARBOR_USER_NAMENAME --password-stdin

docker buildx build --platform linux/amd64,linux/arm64 --build-arg version="${VERSION_STRING}" --rm=true -t $REGISTRY/$NAMESPACE/mqdb:${VERSION_STRING}-${GIT_COMMIT} docker/mqdb --push

docker buildx build --platform linux/amd64,linux/arm64 --build-arg version="${VERSION_STRING}" --rm=true -t $REGISTRY/$NAMESPACE/mqdb-client:${VERSION_STRING}-${GIT_COMMIT} docker/mqdb-client --push

docker buildx build --platform linux/amd64,linux/arm64 --build-arg version="${VERSION_STRING}" --rm=true -t $REGISTRY/$NAMESPACE/mqdb-ssh-client:${VERSION_STRING}-${GIT_COMMIT} docker/mqdb-ssh-client --push

rm -rfv docker/mqdb/clickhouse-*.tgz
rm -rfv docker/mqdb-client/clickhouse-*.tgz
rm -rfv docker/mqdb-ssh-client/clickhouse-*.tgz

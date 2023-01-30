#!/usr/bin/env bash
set -e

source docker/builder/tools/version.sh

cp -rfv artifacts/clickhouse-*.tgz docker/mqdb/

DOCKER_HUB_USER_NAME="myscale"
DOCKER_HUB_USER_PASSWORD="dckr_pat_G5xUzsovxR4NrnaenDqZGtxM6CA"

echo $DOCKER_HUB_USER_PASSWORD | docker login --username $DOCKER_HUB_USER_NAME --password-stdin

docker buildx build --platform linux/amd64,linux/arm64 --build-arg version="${VERSION_STRING}" --rm=true -t $DOCKER_HUB_USER_NAME/mqdb:${VERSION_STRING}-${GIT_COMMIT} docker/mqdb --push

rm -rfv docker/mqdb/clickhouse-*.tgz

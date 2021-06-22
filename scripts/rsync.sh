#/bin/bash

if [ "$#" -ne 1 ]; then
    echo "usage: $0 <user>@<host>"
    exit
fi

rsync --include="~/workspace/ClickHouse/docker/mqdb-builder/Makefile" \
    --include="~/workspace/ClickHouse/contrib/absl/status" \
    --exclude="*/.git" \
    --exclude=".DS_Store" \
    --exclude=build_docker \
    --exclude=cmake-build-relwithdebinfo \
    --exclude=build_docker_single \
    --exclude=cmake-build-debug \
    -avk \
    ~/workspace/upgrade-22.3/ClickHouse \
    $1:~/workspace/upgrade-22.3

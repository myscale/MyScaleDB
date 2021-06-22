#!/usr/bin/env bash

for index in {1..180}; do
    if docker version; then
        break
    fi
    sleep 1
done

docker info

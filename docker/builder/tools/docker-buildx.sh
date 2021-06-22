#!/usr/bin/env bash
set -e

docker run --rm --privileged multiarch/qemu-user-static --reset -p yes >/dev/null 2>&1 || true
docker buildx create --use --name=qemu >/dev/null 2>&1 || true
docker buildx inspect --bootstrap >/dev/null 2>&1 || true

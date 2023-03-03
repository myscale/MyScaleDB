#!/usr/bin/env bash
set -e

docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
docker buildx create --use --name=qemu --driver-opt env.http_proxy='http://clash.internal.moqi.ai:7890' --driver-opt env.https_proxy='http://clash.internal.moqi.ai:7890' --driver-opt '"env.no_proxy='localhost,127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,docker,mirror.ccs.tencentyun.com,git.moqi.ai,.internal.moqi.ai'"'
docker buildx inspect --bootstrap

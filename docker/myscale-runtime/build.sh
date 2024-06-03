#!/usr/bin/env bash
set -e

docker build --rm=true -t harbor.internal.moqi.ai/mqdb/runtime:1.4 .

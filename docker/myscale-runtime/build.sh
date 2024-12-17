#!/usr/bin/env bash
set -e

docker build --rm=true -t harbor.internal.moqi.ai/mqdb/runtime:3.0.0 .

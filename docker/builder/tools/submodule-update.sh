#!/usr/bin/env bash
set -e

source docker/builder/tools/proxy.sh
git clean -d -f -f -x
git submodule sync --recursive
git submodule update --init --force --recursive
git submodule foreach --recursive git clean -d -f -f -x

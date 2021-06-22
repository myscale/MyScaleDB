#!/usr/bin/env bash
set -e
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CURDIR/setup_build.sh

#ccache -s # uncomment to display CCache statistics
cd $CURDIR/../$BUILD_FOLDER/
# Set the number of build jobs to the half of number of virtual CPU cores (rounded up).
# By default, ninja use all virtual CPU cores, that leads to very high memory consumption without much improvement in build time.
# Note that modern x86_64 CPUs use two-way hyper-threading (as of 2018).
# Without this option my laptop with 16 GiB RAM failed to execute build due to full system freeze.

JOBS=$(($(nproc || grep -c ^processor /proc/cpuinfo) - 2))
ninja -j ${JOBS}
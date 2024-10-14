#!/bin/bash
set -e
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CURDIR/setup_build.sh

#ccache -s # uncomment to display CCache statistics
mkdir -p $CURDIR/../$BUILD_FOLDER/
cd $CURDIR/../$BUILD_FOLDER/
export LD_LIBRARY_PATH=/usr/lib/llvm-15/lib:${LD_LIBRARY_PATH}
cmake -G Ninja .. -DCMAKE_C_COMPILER=$(command -v clang-15) \
    -DCMAKE_CXX_COMPILER=$(command -v clang++-15) $SANITIZE_ARGS \
    -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=1 \
    -DENABLE_CLICKHOUSE_ALL=ON \
    -DENABLE_TESTS=ON \
    -DENABLE_THINLTO=OFF \
    -DENABLE_UTILS=OFF \
    -DENABLE_LICENSE_CHECK=OFF \
    -DENABLE_MYSCALE_COMMUNITY_EDITION=OFF \
    -DENABLE_RUST=ON \
    -DBINARY_CPP_DATASET_TEST=OFF

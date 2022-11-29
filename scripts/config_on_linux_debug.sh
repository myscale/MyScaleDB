#!/bin/bash
set -e
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

#ccache -s # uncomment to display CCache statistics
mkdir -p $CURDIR/../build_debug/
cd $CURDIR/../build_debug/
export LD_LIBRARY_PATH=/usr/lib/llvm-13/lib:/opt/intel/oneapi/mkl/2021.4.0/lib/intel64/:${LD_LIBRARY_PATH}
cmake -G Ninja .. -DCMAKE_C_COMPILER=$(command -v clang-13) \
    -DCMAKE_CXX_COMPILER=$(command -v clang++-13) \
    -DCMAKE_BUILD_TYPE=Debug \
    -DENABLE_CLICKHOUSE_ALL=OFF \
    -DENABLE_CLICKHOUSE_SERVER=ON \
    -DENABLE_CLICKHOUSE_CLIENT=ON \
    -DENABLE_CLICKHOUSE_FORMAT=ON \
    -DENABLE_CLICKHOUSE_BENCHMARK=ON \
    -DENABLE_LIBRARIES=OFF \
    -DENABLE_CURL=ON \
    -DENABLE_S3=ON \
    -DENABLE_SSL=ON \
    -DENABLE_REPLXX=ON \
    -DUSE_UNWIND=ON \
    -DENABLE_UTILS=OFF \
    -DENABLE_TESTS=OFF \
    -DENABLE_RAPIDJSON=ON \
    -DENABLE_LICENSE_CHECK=OFF


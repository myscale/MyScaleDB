#!/bin/bash
set -eo pipefail
shopt -s nullglob

args=("$@")
arch="$(dpkg --print-architecture)"
if [[ "x$arch" = "xamd64" ]]; then
    source /opt/intel/oneapi/mkl/$INTEL_ONEAPI_VERSION/env/vars.sh
fi
exec /usr/bin/clickhouse-client "${args[@]}"

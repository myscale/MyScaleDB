#!/usr/bin/env bash
set -e

source docker/builder/tools/version.sh

cp -rfv artifacts/usr/bin/clickhouse docker/mqdb-test/
cp -rfv tests/clickhouse-test docker/mqdb-test/
cp -rfv tests/queries docker/mqdb-test/tests/
cp -rfv tests/performance docker/mqdb-test/tests/
cp -rfv tests/config docker/mqdb-test/tests/

# docker build --rm=true -t mqdb-test docker/mqdb-test/
# docker run -d --rm --name mqdb-test --hostname mqdb-test mqdb-test
# docker exec mqdb-test bash -c "source /opt/intel/oneapi/mkl/$INTEL_ONEAPI_VERSION/env/vars.sh; clickhouse-test $*"
# docker rm -f mqdb-test

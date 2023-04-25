#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/helpers/00000_prepare_index.sh

clickhouse-client -q "SELECT id, vector, distance(vector, [1.0, 1.0, 1.0]) as d FROM test_vector prewhere id < 10 or id > 60 ORDER BY (d, id) limit 20;"

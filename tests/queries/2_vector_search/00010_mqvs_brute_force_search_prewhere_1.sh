#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/helpers/00000_prepare_index_2.sh

clickhouse-client -q "SELECT id, vector, distance(vector, [10020.1, 10020.1, 10020.1]) as d FROM test_vector prewhere id<100 or id>10000 order by d limit 100 SETTINGS enable_brute_force_vector_search=1"

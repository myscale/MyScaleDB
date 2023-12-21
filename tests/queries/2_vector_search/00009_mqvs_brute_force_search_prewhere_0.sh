#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/helpers/00000_prepare_index_2.sh

clickhouse-client -q "SELECT id, vector, distance(vector, [10020.1, 10020.1, 10020.1]) as d FROM test_vector prewhere id>5000 or id =9 or id=31 or id=999 or id=1 order by d limit 100 SETTINGS enable_brute_force_vector_search=1"

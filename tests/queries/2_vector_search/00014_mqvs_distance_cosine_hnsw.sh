#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/helpers/00000_prepare_index_cosine.sh HNSWFLAT

clickhouse-client -q "SELECT id, vector, distance(vector, [0.5, 0.5, 0.5, 0.5]) as d FROM test_vector order by d limit 10;"

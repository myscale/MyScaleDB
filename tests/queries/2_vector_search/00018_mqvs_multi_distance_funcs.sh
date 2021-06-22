#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/helpers/00000_prepare_index.sh

clickhouse-client -q "SELECT id, vector, distance(vector, [0.1, 0.1, 0.1]) as d1, distance(vector, [1.1, 1.1, 1.1]) as d2 FROM test_vector;" 2>&1 | grep -q "DB::Exception: Not support multiple distance funcs in one query now." && echo 'OK' || echo 'FAIL' || :

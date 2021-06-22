#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/helpers/00000_prepare_index.sh

clickhouse-client -q "SELECT id, vector, distance('topK=10')(vectore, [0.1, 0.1, 0.1]) FROM test_vector;" 2>&1 | grep -q "DB::Exception: Missing columns: 'vectore' while processing query" && echo 'OK' || echo 'FAIL' || :

#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/helpers/00000_prepare_data_with_empty_vectors.sh

clickhouse-client -q "SELECT id, vector, distance('topK = 10')(vector, [20.0, 20.0, 20.0]) FROM test_vector;"

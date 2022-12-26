#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/helpers/00000_prepare_data_with_empty_vectors.sh

# test empty vector with IVFFLAT
clickhouse-client -q "SELECT id, vector, distance('topK = 10')(vector, [20.0, 20.0, 20.0]) FROM test_vector;"

# test empty vector with FLAT
clickhouse-client -q "ALTER TABLE test_vector DROP VECTOR INDEX v1;"
clickhouse-client -q "ALTER TABLE test_vector ADD VECTOR INDEX v1 vector TYPE FLAT;"
sleep 1
clickhouse-client -q "SELECT id, vector, distance('topK = 10')(vector, [20.0, 20.0, 20.0]) FROM test_vector;"

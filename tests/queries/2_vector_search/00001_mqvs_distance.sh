#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/helpers/00000_prepare_index.sh

clickhouse-client -q "SELECT id, vector, distance(vector, [0.1, 0.1, 0.1]) FROM test_vector order by distance(vector, [0.1, 0.1, 0.1]) limit 10;"
# detach and attach the table to test deserialization of vector index
clickhouse-client -q "DETACH TABLE test_vector"
clickhouse-client -q "ATTACH TABLE test_vector"
clickhouse-client -q "SELECT id, vector, distance(vector, [0.1, 0.1, 0.1]) FROM test_vector order by distance(vector, [0.1, 0.1, 0.1]) limit 10;"

#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/helpers/00000_prepare_index.sh

clickhouse-client -q "DROP TABLE IF EXISTS test_vector"
clickhouse-client -q "CREATE TABLE test_vector(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) engine MergeTree primary key id"
clickhouse-client -q "SELECT id, distance(vectore, [0.1, 0.1, 0.1]) as d FROM test_vector order by d limit 10;" 2>&1 | grep -q "There is no column 'vectore'." && echo 'OK' || echo 'FAIL' || :
clickhouse-client -q "SELECT [1.0, 1.1, 2.0], distance(vector, [0.1, 0.1, 0.1]) as d, number FROM ( SELECT number FROM system.numbers LIMIT 100) order by d limit 10" 2>&1 | grep -q "DB::Exception: There is no column 'vector'." && echo 'OK' || echo 'FAIL' || :

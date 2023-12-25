#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/helpers/00000_prepare_data_with_empty_vectors.sh

# no enforce_fixed_vector_length_constraint
# test empty vector with IVFFLAT
clickhouse-client -q "SELECT id, vector, distance(vector, [20.0, 20.0, 20.0]) as dist FROM test_vector ORDER BY (dist, id) limit 10;"

# test empty vector with FLAT
clickhouse-client -q "ALTER TABLE test_vector DROP VECTOR INDEX v1;"
clickhouse-client -q "ALTER TABLE test_vector ADD VECTOR INDEX v1 vector TYPE FLAT;"
sleep 1
clickhouse-client -q "SELECT id, vector, distance(vector, [20.0, 20.0, 20.0]) as dist FROM test_vector ORDER BY (dist, id) limit 10;"

# default enforce_fixed_vector_length_constraint
clickhouse-client -q "select table, name, expr, status, latest_failed_part, latest_fail_reason from system.vector_indices where database = currentDatabase() and table like 'test_fail_vector%' order by table;"

clickhouse-client -q "DROP TABLE IF EXISTS test_vector;"
clickhouse-client -q "DROP TABLE IF EXISTS test_fail_vector;"
clickhouse-client -q "DROP TABLE IF EXISTS test_fail_vector_2;"

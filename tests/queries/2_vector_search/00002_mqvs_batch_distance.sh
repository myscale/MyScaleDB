#!/usr/bin/env bash
# Tags: no-parallel

clickhouse-client -q "DROP TABLE IF EXISTS test_vector"
clickhouse-client -q "CREATE TABLE test_vector(id Float32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 3) engine MergeTree primary key id;"
clickhouse-client -q "system stop merges test_vector;"

clickhouse-client -q "INSERT INTO test_vector SELECT number, [number, number, number] FROM numbers(0,50);"
clickhouse-client -q "INSERT INTO test_vector SELECT number, [number, number, number] FROM numbers(50,50);"
clickhouse-client -q "select count(*) from system.parts where (table='test_vector');"

# test metric_type=L2
clickhouse-client -q "ALTER TABLE test_vector ADD VECTOR INDEX v1 vector TYPE HNSWFLAT('metric_type= L2');"
status="NotBuilt"
time=0
while [[ $status != "Built" && $time != 10 ]]; do
        status=$(clickhouse-client -q "select status from system.vector_indices where table = 'test_vector' and name = 'v1';")
        sleep 1
        ((++time))
done
if [ $time -eq 5 ]; then
        echo "fail to build index"
fi
clickhouse-client -q "select '-- batch_distance of metric_type=L2';"
clickhouse-client -q "SELECT id, vector, batch_distance(vector, [[0.1, 0.1, 0.1], [0.2, 0.2, 0.2], [50.1, 50.1, 50.1]]) as dist FROM test_vector order by dist.1,dist.2 limit 10 by dist.1 SETTINGS enable_brute_force_vector_search=1;"

clickhouse-client -q "ALTER TABLE test_vector DROP VECTOR INDEX v1;"

# test metric_type=IP
clickhouse-client -q "ALTER TABLE test_vector ADD VECTOR INDEX v2 vector TYPE HNSWFLAT('metric_type=IP');"
status="NotBuilt"
time=0
while [[ $status != "Built" && $time != 10 ]]; do
        status=$(clickhouse-client -q "select status from system.vector_indices where table = 'test_vector' and name = 'v2';")
        sleep 1
        ((++time))
done
if [ $time -eq 5 ]; then
        echo "fail to build index"
fi
clickhouse-client -q "select '-- batch_distance of metric_type=IP';"
clickhouse-client -q "SELECT id, vector, batch_distance(vector, [[0.1, 0.1, 0.1], [0.2, 0.2, 0.2], [50.1, 50.1, 50.1]]) as dist FROM test_vector order by dist.1,dist.2 DESC limit 10 by dist.1 SETTINGS enable_brute_force_vector_search=1;"

clickhouse-client -q "DROP TABLE IF EXISTS test_vector"


#!/usr/bin/env bash
# Tags: no-parallel

max_response_time_drop_vector_index=1
max_response_time_drop_table=10

# create table & insert data
clickhouse-client -q "DROP TABLE IF EXISTS test_drop_table;"
clickhouse-client -q "CREATE TABLE test_drop_table(id UInt32, text String, vector FixedArray(Float32, 768)) Engine MergeTree ORDER BY id;"
clickhouse-client -q "INSERT INTO test_drop_table SELECT number, randomPrintableASCII(80), range(768) FROM numbers(500000);"
clickhouse-client -q "optimize table test_drop_table final;"

# when building vector index, drop this vector index
# drop vector index v1
clickhouse-client -q "ALTER TABLE test_drop_table ADD VECTOR INDEX v1 vector TYPE HNSWSQ;"
clickhouse-client -q "select table, name, type, expr, status from system.vector_indices where database = currentDatabase() and table = 'test_drop_table';"
sleep 3
clickhouse-client -q "ALTER TABLE test_drop_table DROP VECTOR INDEX v1;"
clickhouse-client -q "select '-- Empty result, no vector index v1';"
clickhouse-client -q "select table, name, type, expr, status from system.vector_indices where database = currentDatabase() and table = 'test_drop_table';"
sleep $max_response_time_drop_vector_index

# create vector index v2
clickhouse-client -q "ALTER TABLE test_drop_table ADD VECTOR INDEX v2 vector TYPE HNSWFLAT;"
status="NotBuilt"
while [[ $status != "Built" ]]
do
        status=`clickhouse-client -q "select status from system.vector_indices where table = 'test_drop_table' and name = 'v2';"`
        if [ $(clickhouse-client -q "select 1" 2>&1 | grep "Connection refused" | wc -l) == "1" ]; then
            exit 1;
        fi
        sleep 1
done
clickhouse-client -q "select '-- Create a new vector index v2 with different name and different type';"
clickhouse-client -q "select table, name, type, expr, status from system.vector_indices where database = currentDatabase() and table = 'test_drop_table';"
clickhouse-client -q "ALTER TABLE test_drop_table DROP VECTOR INDEX v2;"


# when building vector index, drop the table
clickhouse-client -q "ALTER TABLE test_drop_table ADD VECTOR INDEX v3 vector TYPE HNSWPQ;"
sleep 3
time_start=`date +%s`
clickhouse-client -q "DROP TABLE IF EXISTS test_drop_table;"
time_end=`date +%s`
time_interval=$(( $time_end - $time_start ))
if [ $time_interval -lt $max_response_time_drop_table ]
    then echo 'ok'
fi

#!/usr/bin/env bash
# Tags: no-parallel

echo 'mutation on mergetree can reuse vector index'
clickhouse-client -q "drop table if exists test_mutation sync"
clickhouse-client -q "create table test_mutation (id Int32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 480) ENGINE = MergeTree ORDER BY id settings min_rows_for_wide_part=5000, min_rows_for_compact_part=1000, min_bytes_for_wide_part=0, min_bytes_for_compact_part=0"
clickhouse-client -q "alter table test_mutation add vector index replia_ind vector type MSTG"

clickhouse-client -q "insert into test_mutation select number, range(480) from numbers(1000)"
clickhouse-client -q "insert into test_mutation select number, range(480) from numbers(1000,5000)"
clickhouse-client -q "insert into test_mutation select number, range(480) from numbers(6000,1000)"

clickhouse-client -q "SELECT name, part_type from system.parts where database=currentDatabase() and table='test_mutation'"
status="InProgress"
time=0
while [[ $status != "Built" && $time != 5 ]]
do
        status=`clickhouse-client -q "select status from system.vector_indices where table = 'test_mutation' and name = 'replia_ind'"`
        sleep 3
        ((++time))
done
if [ $time -eq 5 ]; then
        echo "fail to build index for test_mutation"
fi

clickhouse-client -q "SELECT total_parts, parts_with_vector_index, status FROM system.vector_indices WHERE table='test_mutation' and database=currentDatabase()"

# check vector index status after lightweight delete
clickhouse-client --allow_experimental_lightweight_delete=1 --mutations_sync=1 -q "delete from test_mutation where id in (10, 1002)"
clickhouse-client -q "SELECT total_parts, parts_with_vector_index, status FROM system.vector_indices WHERE table='test_mutation' and database=currentDatabase()"

clickhouse-client -q "DROP TABLE test_mutation sync"

echo 'mutation on replicated mergetree can reuse vector index'
clickhouse-client -q "drop table if exists test_replica_mutation sync"
clickhouse-client -q "create table test_replica_mutation (id Int32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 480) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/mqvs_00022/test_mutation', 'r1') ORDER BY id settings min_rows_for_wide_part=5000, min_rows_for_compact_part=1000, min_bytes_for_wide_part=0, min_bytes_for_compact_part=0"
clickhouse-client -q "alter table test_replica_mutation add vector index replia_ind vector type MSTG"

clickhouse-client -q "insert into test_replica_mutation select number, range(480) from numbers(1001)"
clickhouse-client -q "insert into test_replica_mutation select number, range(480) from numbers(1001,5000)"

clickhouse-client -q "SELECT name, part_type from system.parts where database=currentDatabase() and table='test_replica_mutation'"

# wait build index finish
status="InProgress"
time=0
while [[ $status != "Built" && $time != 5 ]]
do
        status=`clickhouse-client -q "select status from system.vector_indices where table = 'test_replica_mutation' and name = 'replia_ind'"`
        sleep 3
        ((++time))
done
if [ $time -eq 5 ]; then
        echo "fail to build index for test_replica_mutation"
fi

clickhouse-client -q "SELECT total_parts, parts_with_vector_index, status FROM system.vector_indices WHERE table='test_replica_mutation' and database=currentDatabase()"

# check vector index status after lightweight delete
clickhouse-client --allow_experimental_lightweight_delete=1 --mutations_sync=1 -q "delete from test_replica_mutation where id in (10, 1002)"
clickhouse-client -q "SELECT total_parts, parts_with_vector_index, status FROM system.vector_indices WHERE table='test_replica_mutation' and database=currentDatabase()"

clickhouse-client -q "DROP TABLE test_replica_mutation sync"

echo 'mutation on replicated mergetree does not cancel building vector index'
clickhouse-client -q "drop table if exists test_replica_mutation_cancel sync"
clickhouse-client -q "create table test_replica_mutation_cancel (id Int32, vector Array(Float32), CONSTRAINT vector_len CHECK length(vector) = 480) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/mqvs_00022/test_mutation_cancel', 'r1') ORDER BY id settings min_rows_for_wide_part=6000, min_rows_for_compact_part=1000, min_bytes_for_wide_part=0, min_bytes_for_compact_part=0"
clickhouse-client -q "alter table test_replica_mutation_cancel add vector index replia_ind vector type MSTG"

clickhouse-client -q "insert into test_replica_mutation_cancel select number, range(480) from numbers(50000)"
sleep 1
clickhouse-client --allow_experimental_lightweight_delete=1 --mutations_sync=1 -q "delete from test_replica_mutation_cancel where id < 1000"

# check build index is not cancled by mutation
status="InProgress"
time=0
while [[ $status != "Built" && $status != "Error" && $time < 5 ]]
do
        status=`clickhouse-client -q "select status from system.vector_indices where table = 'test_replica_mutation_cancel' and name = 'replia_ind'"`
        sleep 3
        ((++time))
done

if [[ $status != "InProgress" && $status != "Built" ]]; then
        echo "build index of old part is canceled"
fi

clickhouse-client -q "SELECT latest_failed_part FROM system.vector_indices WHERE table='test_replica_mutation' and database=currentDatabase()"
clickhouse-client -q "DROP TABLE test_replica_mutation_cancel sync"

# Tags: no-parallel

clickhouse-client -q "SELECT '-- Test single vector index';"

clickhouse-client -q "DROP TABLE IF EXISTS test_single_vindex;"
clickhouse-client -q "CREATE TABLE test_single_vindex (id UInt32, v1 Array(Float32), VECTOR INDEX v1 v1 TYPE MSTG, CONSTRAINT v1_len CHECK length(v1)=3) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_to_build_vector_index=10000;"

clickhouse-client -q "INSERT INTO test_single_vindex SELECT number, [number, number, number] FROM numbers(5500);"

clickhouse-client -q "INSERT INTO test_single_vindex SELECT number, [number, number, number] FROM numbers(5500, 5500);"

status="InProgress"
time=0
while [ "$status" != "Built" ] && [ $time -lt 5 ]
do
  status=`clickhouse-client -q "select status from system.vector_indices where table = 'test_single_vindex' and name = 'v1'"`
  sleep 2
  time=$((time + 1))
done
if [ $time -eq 5 ]; then
  echo "fail to build index for test_single_vindex"
fi

part_count=$(clickhouse-client -q "SELECT count() FROM system.parts WHERE table='test_single_vindex' and database=currentDatabase() and active=1")

if [ $part_count -ne 1 ]; then
  clickhouse-client -q "optimize table test_single_vindex final;"
  status="InProgress"
  time=0
  while [ "$status" != "Built" ] && [ $time -lt 5 ]
  do
  status=`clickhouse-client -q "select status from system.vector_indices where table = 'test_single_vindex' and name = 'v1'"`
  sleep 2
  time=$((time + 1))
  done
  if [ $time -eq 5 ]; then
  echo "fail to build index for test_single_vindex"
  fi
fi

old_part_name=$(clickhouse-client -q "SELECT name FROM system.parts WHERE table='test_single_vindex' and database=currentDatabase() and active=1")

clickhouse-client -q "SELECT table, name, status, total_vectors, part, owner_part FROM system.vector_index_segments WHERE table='test_single_vindex';"

clickhouse-client -q "SELECT table, name, status, total_vectors, part, owner_part FROM system.vector_index_segments WHERE table='test_single_vindex';"

clickhouse-client -q "ALTER TABLE test_single_vindex DETACH PART '$old_part_name';"

clickhouse-client -q "SELECT table, name, status, total_vectors, part, owner_part FROM system.vector_index_segments WHERE table='test_single_vindex';"

clickhouse-client -q "ALTER TABLE test_single_vindex ATTACH PART '$old_part_name';"

clickhouse-client -q "SELECT id, distance(v1, [1.2, 2.3, 3.4]) AS dist FROM test_single_vindex ORDER BY dist LIMIT 10;"

clickhouse-client -q "SELECT table, name, status, total_vectors, part, owner_part FROM system.vector_index_segments WHERE table='test_single_vindex';"

#!/usr/bin/env bash
# Tags: no-parallel

echo 'vector index on s3'

clickhouse-client -q "drop table if exists test_vector_index_s3 sync"
clickhouse-client -q "create table test_vector_index_s3 (id Int32, vector Array(Float32), CONSTRAINT check_length CHECK length(vector) = 3) ENGINE = MergeTree ORDER BY id settings enable_rebuild_for_decouple=false, storage_policy='s3_cache'"
clickhouse-client -q "alter table test_vector_index_s3 add vector index vec_ind vector type IVFFLAT"

clickhouse-client -q "insert into test_vector_index_s3 select number, [number,number,number] from numbers(100)"

status="InProgress"
time=0
while [[ $status != "Built" && $time != 5 ]]
do
  status=`clickhouse-client -q "select status from system.vector_indices where table = 'test_vector_index_s3' and name = 'vec_ind'"`
  sleep 2
  ((++time))
done
if [ $time -eq 5 ]; then
  echo "fail to build index for test_vector_index_s3"
fi

clickhouse-client -q "SELECT id, distance(vector, [1.2, 2.3, 3.4]) AS dist FROM test_vector_index_s3 order by dist limit 10;"

part_dir=`clickhouse-client -q "SELECT path FROM system.parts WHERE table='test_vector_index_s3' and database=currentDatabase() and active=1"`

if [ -z "$part_dir" ]; then
  echo "fail to get data part dir"
fi

# normally s3 object metadata file is around 50-60 bytes
# if file size is greater than 70 bytes, then it must be a normal file (not a s3 metadata file)
function check_s3() {
  for f in $1/*; do
    if [ $(stat -c%s $f) -ge 70 ]; then
        echo "$(basename $f) not in s3"
    fi
  done
}

# check if all files in data part directory were uploaded to s3
check_s3 $part_dir


echo 'lightweight delete'

clickhouse-client -q "delete from test_vector_index_s3 where id=3"
clickhouse-client -q "SELECT id, distance(vector, [1.2, 2.3, 3.4]) AS dist FROM test_vector_index_s3 order by dist limit 10;"

lwd_dir=`clickhouse-client -q "SELECT path FROM system.parts WHERE table='test_vector_index_s3' and database=currentDatabase() and active=1"`

if [ "$lwd_dir" == "$part_dir" ]; then
  echo "part dir not changed after lwd"
fi

# check if all files were uploaded to s3 after lwd
check_s3 $lwd_dir


echo 'merge'

clickhouse-client -q "insert into test_vector_index_s3 select number, [number,number,number] from numbers(100,100)"
status="InProgress"
time=0
while [[ $status != "Built" && $time != 5 ]]
do
  status=`clickhouse-client -q "select status from system.vector_indices where table = 'test_vector_index_s3' and name = 'vec_ind'"`
  sleep 2
  ((++time))
done
if [ $time -eq 5 ]; then
  echo "fail to build index for test_vector_index_s3"
fi

clickhouse-client -q "optimize table test_vector_index_s3 final"
clickhouse-client -q "SELECT id, distance(vector, [1.2, 2.3, 3.4]) AS dist FROM test_vector_index_s3 order by dist limit 10;"

merged_dir=`clickhouse-client -q "SELECT path FROM system.parts WHERE table='test_vector_index_s3' and database=currentDatabase() and active=1"`

if [ "$merged_dir" == "$lwd_dir" ]; then
  echo "part dir not changed after merge"
fi

# check if all files were uploaded to s3 in case of decoupled parts
check_s3 $merged_dir

row_ids_map_exists=0
inverted_row_ids_map_exists=0
inverted_row_sources_map_exists=0

# check if all files related to decoupled parts exists
for f in $merged_dir/*; do
  if [[ "$(basename $f)" == *"-row_ids_map.vidx3" ]]; then
    row_ids_map_exists=1
    continue
  fi

  if [[ "$(basename $f)" == "merged-inverted_row_ids_map.vidx3" ]]; then
    inverted_row_ids_map_exists=1
    continue
  fi

  if [[ "$(basename $f)" == "merged-inverted_row_sources_map.vidx3" ]]; then
    inverted_row_sources_map_exists=1
  fi
done

if [ $row_ids_map_exists -eq 0 ]; then
  echo "row_ids_map.vidx3 not found"
fi

if [ $inverted_row_ids_map_exists -eq 0 ]; then
  echo "merged-inverted_row_ids_map.vidx3 not found"
fi

if [ $inverted_row_sources_map_exists -eq 0 ]; then
  echo "merged-inverted_row_sources_map.vidx3 not found"
fi

clickhouse-client -q "DROP TABLE test_vector_index_s3 sync"

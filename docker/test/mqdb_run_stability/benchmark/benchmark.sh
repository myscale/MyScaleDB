#!/bin/bash
while true
do
    status=$(clickhouse-client -q "select status from system.vector_indices where table='gist960_100w'" -h $HOST)
    if [ "$status" = "Built" ]; then
        clickhouse-benchmark -c $PARALLEL --timelimit=$TIME_OUT --randomize=1 -h $HOST < bench_query.sql
        continue
    else
        echo "$status"
        sleep 10
    fi
done
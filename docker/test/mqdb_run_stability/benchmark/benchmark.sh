#!/bin/bash
echo "mqdb_host is ${HOST}, new_sql is ${NEW_SQL}, timeout is ${TIME_OUT}s"
while true
do
    status=$(clickhouse-client -q "select status from system.vector_indices where table = 'testdata_baidu_100w'" -h $HOST)
    if [ "$status" = "Built" ]; then
        if [ "$NEW_SQL" = "1" ]; then
            clickhouse-benchmark -c $PARALLEL --timelimit=$TIME_OUT --randomize=1 < bench_query_new.sql -h $HOST
            continue
        else
            clickhouse-benchmark -c $PARALLEL --timelimit=$TIME_OUT --randomize=1 < bench_query_old.sql -h $HOST
            continue
        fi
    else
        echo "$status"
        sleep 3
    fi
done
#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="SELECT number, if(number<699999, toString(number), 'a') FROM numbers(700000) FORMAT CSV" > "${CLICKHOUSE_TMP}"/data_for_atomic_insert.csv
${CLICKHOUSE_CLIENT} --query="CREATE TABLE t_atomic(a int, b int) engine=MergeTree order by a"
${CLICKHOUSE_CLIENT} --query="INSERT INTO t_atomic(a,b) settings atomic_insert=true FORMAT CSV" < "${CLICKHOUSE_TMP}"/data_for_atomic_insert.csv 2>&1 | grep -q "DB::Exception: Insert query has been canceled." && echo 'OK' || echo 'Fail' || :

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM t_atomic"

${CLICKHOUSE_CLIENT} --query="DROP TABLE t_atomic"

# delete all created elements
rm -f "${CLICKHOUSE_TMP}"/data_for_atomic_insert.csv

#!/usr/bin/env bash
set -e


function getConfig() {
    export REPLICA=$( shyaml get-value server.replica < config.yaml )
    export CHI_NAME=$( shyaml get-value server.chi_name < config.yaml )
    export CLUSTER_NAME=$( shyaml get-value server.cluster_name < config.yaml )
    export NAMESPACE=$( shyaml get-value server.namespace < config.yaml )
    export TABLE_NAME=$( shyaml get-value table_name < config.yaml )
    export HOST_PREFIX="chi-"${CHI_NAME}"-"${CLUSTER_NAME}"-0-"
}

function runBenchmark() {
    clickhouse-benchmark -c 32 -h ""$HOST_PREFIX""$1"" --continue_on_errors < queries/benchmark_query.sql
}

echo "get cluster config"
getConfig
find ./chaos-mesh -type f -name "*.yaml" -print0 | xargs -0 sed -i "s?CHAOS_NAMESPACE?$NAMESPACE?"

echo "start operation consistency and data integrity check"
python3 runner/run.py --config-file config.yaml consistency
echo "finish consistency check"

echo "start run benchmark query"
for (( i=0; i<$REPLICA; i++ )); do
    runBenchmark $i &>/dev/null &
done

sleep 3m
echo "start qps performance check"
python3 runner/run.py --config-file config.yaml performance

echo "start multi process test"
locust -f runner/multi.py --headless --host localhost -u 100 -r 50 --only-summary -t 10m
sleep 15m

echo "check replicas data count"
python3 runner/run.py --config-file config.yaml check

echo "finish chaos test"

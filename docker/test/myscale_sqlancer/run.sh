#!/bin/bash
set -exu
trap "exit" INT TERM

TEST_OUTPUT=$WORKPATH/test_output
arch="amd64" #hardcoded for now

# install clickhouse deb from artifacts
function install_clickhouse_deb
{
    dpkg -i $WORKPATH/packages/clickhouse-common-static_*$arch.deb
    dpkg -i $WORKPATH/packages/clickhouse-common-static-dbg_*$arch.deb
    dpkg -i $WORKPATH/packages/clickhouse-server_*$arch.deb
    dpkg -i $WORKPATH/packages/clickhouse-client_*$arch.deb
}

install_clickhouse_deb

echo '<clickhouse><listen_host>0.0.0.0</listen_host></clickhouse>' >/etc/clickhouse-server/config.d/listen.xml
echo '<clickhouse><interserver_listen_host>0.0.0.0</interserver_listen_host></clickhouse>' >/etc/clickhouse-server/config.d/interserver_listen_host.xml

cd $WORKPATH
clickhouse server -P $TEST_OUTPUT/clickhouse-server.pid -L $TEST_OUTPUT/clickhouse-server.log -E $TEST_OUTPUT/clickhouse-server.log.err --daemon

for _ in $(seq 1 60); do if [[ $(wget -q 'localhost:8123' -O-) == 'Ok.' ]]; then break ; else sleep 1; fi ; done

cd /sqlancer/sqlancer-main

TIMEOUT=300
NUM_QUERIES=1000
NUM_THREADS=10
TESTS=( "TLPGroupBy" "TLPHaving" "TLPWhere" "TLPDistinct" "TLPAggregate" "NoREC" )
echo "${TESTS[@]}"

for TEST in "${TESTS[@]}"; do
    echo "$TEST"
    if [[ $(wget -q 'localhost:8123' -O-) == 'Ok.' ]]
    then
        echo "Server is OK"
        ( java -jar target/sqlancer-*.jar --log-each-select true --print-failed false --num-threads "$NUM_THREADS" --timeout-seconds "$TIMEOUT" --num-queries "$NUM_QUERIES"  --username default --password "" clickhouse --oracle "$TEST" | tee "$TEST_OUTPUT/$TEST.out" )  3>&1 1>&2 2>&3 | tee "$TEST_OUTPUT/$TEST.err"
    else
        touch "$TEST_OUTPUT/$TEST.err" "$TEST_OUTPUT/$TEST.out"
        echo "Server is not responding" | tee $TEST_OUTPUT/server_crashed.log
    fi
done

ls $TEST_OUTPUT
pkill -F $TEST_OUTPUT/clickhouse-server.pid || true

for _ in $(seq 1 60); do if [[ $(wget -q 'localhost:8123' -O-) == 'Ok.' ]]; then sleep 1 ; else break; fi ; done

/process_sqlancer_result.py --in-results-dir $TEST_OUTPUT \
    --out-results-file $TEST_OUTPUT/summary.tsv \
    --out-description-file $TEST_OUTPUT/description.txt \
    --out-status-file $TEST_OUTPUT/status.txt || echo -e "failure\tCannot parse results" > $TEST_OUTPUT/check_status.tsv
ls $TEST_OUTPUT 

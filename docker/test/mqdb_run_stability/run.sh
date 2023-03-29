#!/bin/bash
set -e

export KUBECONFIG=$KUBECONFIG:$HOME/Downloads/cls-config
source docker/builder/tools/version.sh

# CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# PROJECT_PATH=$CUR_DIR/../../..

# default namespace "stability-test"
default_namespace='stability-test'
# default_grafana_url="http://GRAFANA_IP/d/clickHouse/clickhouse?orgId=1&var-datasource=Prometheus&var-namespace=STABILITY_TEST&var-chi=All&var-cluster=All&var-trends=1&var-peaks=null&from=TIME_START&to=TIEM_END"
GRAFANA_URL=''

current_namespace=${default_namespace}
mqdb_server_ip='127.0.0.1'
tiemout=${1:-259200}
# tiemout=259200
function create_namespace()
{
    log_file_counter=0
    kubectl get namespace
    cur=$(kubectl get namespace | grep "${current_namespace}" | wc -l)
    while [ $cur -ne 0 ]
    do
        log_file_counter=$((log_file_counter + 1))
        current_namespace="${default_namespace}-${log_file_counter}"
        cur=$(kubectl get namespace | grep "${current_namespace}" | wc -l)
    done
    kubectl create namespace "${current_namespace}"
}

create_namespace

function setup_mqdb()
{
    cp -f docker/test/mqdb_run_stability/mqdb_server.yaml docker/test/mqdb_run_stability/mqdb_server_tmp.yaml
    sed -i "s?IMAGE_VERSION?$VERSION_STRING-$GIT_COMMIT?" docker/test/mqdb_run_stability/mqdb_server_tmp.yaml
    sed -i "s?STABILITY_NAMESACE?$current_namespace?" docker/test/mqdb_run_stability/mqdb_server_tmp.yaml
    kubectl apply -f docker/test/mqdb_run_stability/mqdb_server_tmp.yaml
    sleep 60
    if ! kubectl wait pods chi-testing-testing-0-0-0 -n $current_namespace --for=condition=ready --timeout=3000s; then echo "setup mqdb server faild" && kubectl delete ns $current_namespace; exit 1; fi
    mqdb_server_ip=$(kubectl get services -n $current_namespace | grep 'clickhouse-testing' | awk '{print $4}')
}

setup_mqdb

# start_time=$[$(date +%s%N)/1000000]
start_time="$(date +%s)000"

# setup benchmark
docker/test/mqdb_run_stability/run_benchmark.sh ${mqdb_server_ip} $tiemout


# setup lwd
docker/test/mqdb_run_stability/run_lwd.sh ${mqdb_server_ip} $tiemout

function generate_grafana_url()
{
    GRAFANA_IP=$(kubectl get services -n monitoring | grep '^grafana' | awk '{print $4}')
    NAMESPACE=${current_namespace}
    START=${1}
    END=${2}
    GRAFANA_URL="http://${GRAFANA_IP}/d/clickHouse/clickhouse?orgId=1&var-datasource=Prometheus&var-namespace=${NAMESPACE}&var-chi=All&var-cluster=All&var-trends=1&var-peaks=null&from=${START}&to=${END}"
}   


function check_server_status()
{
    RESTARTS=0
    cur_time=0
    while [ $RESTARTS -eq 0 ] && [ $cur_time -lt $tiemout ]
    do
        echo 'The server status is normal! Will retest after 1h '
        cur_time=$((cur_time + 3600))
        sleep 3600
        RESTARTS=$(kubectl get pods -n $current_namespace | grep 'chi' | awk '{print $4}')
    done

    if [ $RESTARTS -ne 0 ]
    then
        echo 'Mqdb server has restarted and needs to be analyzed'
        end_time="$(date +%s)000"
        generate_grafana_url $start_time $end_time 
        echo "Grafana data can be viewed through the following url: $GRAFANA_URL"
        exit 1
    else
        echo 'The long-term stability test is over, and the server does not restart!'
    fi
}

check_server_status

#!/bin/bash
set -e

export KUBECONFIG=$KUBECONFIG:$HOME/Downloads/cls-config
# export KUBECONFIG=$KUBECONFIG:$HOME/Downloads/cls-b2xd2os9-config
source docker/builder/tools/version.sh

default_namespace=${1:-'stability-test'}

current_namespace=${default_namespace}
mqdb_server_ip='127.0.0.1'
timeout=${2:-259200}

# default_grafana_url="http://GRAFANA_IP/d/clickHouse/clickhouse?orgId=1&var-datasource=Prometheus&var-namespace=STABILITY_TEST&var-chi=All&var-cluster=All&var-trends=1&var-peaks=null&from=TIME_START&to=TIEM_END"
GRAFANA_URL=''

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
    echo "Current test environment namespace: ${current_namespace}"
    kubectl create namespace "${current_namespace}"
}

create_namespace

function get_mqdb_host()
{
    host=""
    retry=0
    while [ -z $host ] && [ $retry -lt 10 ]; do
        echo "Waiting for external IP"
        host=$(kubectl get svc $1 --namespace ${current_namespace} --template="{{range .status.loadBalancer.ingress}}{{.ip}}{{end}}")
        [ -z "$host" ] && sleep 10
        retry=$((retry + 1))
    done
    if [ ! -z $host ];then
        echo 'Found external IP: '$host
        mqdb_server_ip=$host
    else
        echo 'Get external IP Error!'
        exit 1
    fi
}
function prepare_nvme_node()
{
    cp -f docker/test/mqdb_run_stability/prepare_nvme_node.yaml docker/test/mqdb_run_stability/prepare_nvme_node_tmp.yaml
    sed -i "s?STABILITY_NAMESACE?$current_namespace?" docker/test/mqdb_run_stability/prepare_nvme_node_tmp.yaml
    kubectl apply -f docker/test/mqdb_run_stability/prepare_nvme_node_tmp.yaml
    sleep 60
    if ! kubectl wait --for=condition=Ready pod/prepare-nvme-node -n $current_namespace --timeout=3000s; then echo "setup nvme node error, will not run stability test" && kubectl delete ns $current_namespace; exit 1; fi
    echo "setup nvme node success"
    kubectl get nodes
}

function delete_prepare_pod
{
    kubectl delete -f docker/test/mqdb_run_stability/prepare_nvme_node_tmp.yaml
}

function setup_mqdb()
{
    cp -f docker/test/mqdb_run_stability/mqdb_server.yaml docker/test/mqdb_run_stability/mqdb_server_tmp.yaml
    # cp -f docker/test/mqdb_run_stability/zookeeper.yaml docker/test/mqdb_run_stability/zookeeper_tmp.yaml
    sed -i "s?IMAGE_VERSION?$VERSION_STRING-$GIT_COMMIT?" docker/test/mqdb_run_stability/mqdb_server_tmp.yaml
    # sed -i "s?IMAGE_VERSION?$VERSION_STRING-77052f97?" docker/test/mqdb_run_stability/mqdb_server_tmp.yaml
    sed -i "s?STABILITY_NAMESACE?$current_namespace?" docker/test/mqdb_run_stability/mqdb_server_tmp.yaml
    cp -f docker/test/mqdb_run_stability/mqdb_server_tmp.yaml docker/test/mqdb_run_stability/test_output/mqdb_server_setup.yaml
    # sed -i "s?STABILITY_NAMESACE?$current_namespace?" docker/test/mqdb_run_stability/zookeeper_tmp.yaml
    # kubectl apply -f docker/test/mqdb_run_stability/zookeeper_tmp.yaml
    # sleep 60
    # if ! kubectl wait -l statefulset.kubernetes.io/pod-name=zookeeper-0 --for=condition=ready pod -n $current_namespace --timeout=300s; then echo "setup zookeeper faild" && kubectl delete ns $current_namespace; exit 1; fi
    kubectl apply -f docker/test/mqdb_run_stability/mqdb_server_tmp.yaml
    sleep 60
    if ! kubectl wait -l statefulset.kubernetes.io/pod-name=chi-testing-testing-0-0-0 --for=condition=ready pod -n $current_namespace --timeout=3000s; then echo "setup mqdb server faild" && kubectl delete ns $current_namespace; exit 1; fi
    sleep 60
    get_mqdb_host clickhouse-testing
}


setup_mqdb
echo "mqdb server ip: $mqdb_server_ip, time out: $timeout"

# start_time=$[$(date +%s%N)/1000000]
start_time="$(date +%s)000"

# setup benchmark
docker/test/mqdb_run_stability/run_benchmark.sh ${mqdb_server_ip} $timeout


# setup lwd
docker/test/mqdb_run_stability/run_lwd.sh ${mqdb_server_ip} $timeout "MSTG"

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
    while [ $RESTARTS -eq 0 ] && [ $cur_time -lt $timeout ]
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
        end_time="$(date +%s)000"
        generate_grafana_url $start_time $end_time 
        echo "Grafana data can be viewed through the following url: $GRAFANA_URL"
    fi
}

check_server_status

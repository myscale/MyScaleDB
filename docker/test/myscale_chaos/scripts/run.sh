#!/bin/bash
set -e

export KUBECONFIG=$KUBECONFIG:$HOME/Downloads/cls-config
source docker/builder/tools/version.sh

default_namespace='chaos-ci'
GRAFANA_URL=''
JOB_LOKI_URL=''
current_namespace=${default_namespace}
timeout=${1:-14400}

function generate_urls()
{
    GRAFANA_IP=$(kubectl get services -n monitoring | grep '^grafana' | awk '{print $4}')
    NAMESPACE=${current_namespace}
    START=${1}
    END=${2}
    GRAFANA_URL="http://${GRAFANA_IP}/d/clickHouse/clickhouse?orgId=1&var-datasource=Prometheus&var-namespace=${NAMESPACE}&var-chi=All&var-cluster=All&var-trends=1&var-peaks=null&from=${START}&to=${END}"
    JOB_LOKI_URL="http://${GRAFANA_IP}/explore?orgId=1&left={\"datasource\":\"P8E80F9AEF21F6940\",\"queries\":[{\"refId\":\"A\",\"datasource\":{\"type\":\"loki\",\"uid\":\"P8E80F9AEF21F6940\"},\"editorMode\":\"builder\",\"expr\":\"{namespace%3D\\\"${NAMESPACE}\\\", job%3D\\\"${NAMESPACE}%2Fchaos-test\\\"} |%3D \`\`\",\"queryType\":\"range\"}],\"range\":{\"from\":\"${START}\",\"to\":\"${END}\"}}"
}

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

function setup_mqdb()
{
  cp -f docker/test/myscale_chaos/deploy/chaos-runner.yaml docker/test/myscale_chaos/deploy/chaos-runner-tmp.yaml
  cp -f docker/test/myscale_chaos/deploy/mqdb-replicated.yaml docker/test/myscale_chaos/deploy/mqdb-replicated-tmp.yaml
  sed -i "s?BASE_IMAGE?$BASE_IMAGE?" docker/test/myscale_chaos/deploy/mqdb-replicated-tmp.yaml
  sed -i "s?UDF_IMAGE_VERSION?$UDF_IMAGE_VERSION?" docker/test/myscale_chaos/deploy/mqdb-replicated-tmp.yaml
  sed -i "s?IMAGE_VERSION?$VERSION_STRING-$GIT_COMMIT?" docker/test/myscale_chaos/deploy/chaos-runner-tmp.yaml
  sed -i "s?CHAOS_TIMEOUT?$CHAOS_TIMEOUT?" docker/test/myscale_chaos/deploy/chaos-runner-tmp.yaml
  sed -i "s?BASE_IMAGE?$BASE_IMAGE?" docker/test/myscale_chaos/deploy/chaos-runner-tmp.yaml
  sed -i "s?CHAOS_NAMESPACE?$current_namespace?" docker/test/myscale_chaos/deploy/mqdb-replicated-tmp.yaml
  sed -i "s?CHAOS_NAMESPACE?$current_namespace?" docker/test/myscale_chaos/deploy/chaos-runner-tmp.yaml
}

function check_fatal()
{
  for (( i=0; i<2; i++ )); do
      set +e
      kubectl exec -n ${current_namespace} "chi-chaos-test-clickhouse-0-"$i"-0" -- clickhouse-client -q 'select count(*) from system.crash_log'
      if [ $? -eq 60 ]
      then
        echo "replica$i not found fatal error"
      else
        echo "replica$i got fatal error"
        exit 1
      fi
  done
}

function check_job_status()
{
  cur_time=0
  while [ $cur_time -lt $timeout ]
  do
      state=$( kubectl get pod -n ${current_namespace} --selector=jobgroup=chaos-test --no-headers | awk '{print $3}')
      if [ "$state" == "Error" ]
      then
        end_time="$(date +%s)000"
        echo "job has failed and needs to be analyzed"
        echo "show recent job logs"
        kubectl logs -n ${current_namespace} --tail=30 -l jobgroup=chaos-test
        generate_urls $start_time $end_time
        echo "grafana url: $GRAFANA_URL"
        echo "job log url: $JOB_LOKI_URL"
        echo "check fatal error"
        check_fatal
        exit 1
      elif [ "$state" == "Completed" ]
      then
        echo "job has completed successfully"
        end_time="$(date +%s)000"
        generate_urls $start_time $end_time
        echo "grafana url: $GRAFANA_URL"
        echo "job log url: $JOB_LOKI_URL"
        echo "check fatal error"
        check_fatal
        kubectl delete ns $current_namespace
        exit 0
      else
        echo "job is still running, will recheck after 5m"
        cur_time=$((cur_time + 300))
        sleep 300
      fi
  done
  echo "job timeout"
}

echo "create namespace"
create_namespace
echo "setup mqdb cluster"
setup_mqdb
kubectl apply -f docker/test/myscale_chaos/deploy/mqdb-replicated-tmp.yaml

echo "wait cluster running"
if ! kubectl wait --for=jsonpath='{.status.status}'=Completed  --timeout=1200s -n "${current_namespace}" chi/chaos-test;
then
  echo "failed to create mqdb cluster within 20m"
  kubectl delete ns $current_namespace
  exit 1
fi
# just in case keeper is not ready
kubectl rollout status --watch --timeout=600s -n "${current_namespace}" statefulset/clickhouse-keeper

start_time="$(date +%s)000"
echo "apply chaos runner job"
kubectl apply -f docker/test/myscale_chaos/deploy/chaos-runner-tmp.yaml
echo "start checking job status"
check_job_status
kubectl delete ns $current_namespace

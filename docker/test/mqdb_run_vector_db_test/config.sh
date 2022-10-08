set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HOST=${1:-127.0.0.1}
MQDB_VERSION=${2:-v22.3.7.5_xxxxxxx}
TEST_PALTFORM=${3:-K8S_4c_8g_myscale_testing}

cp $CUR_DIR/vector-db-test-job.yaml $CUR_DIR/vector-db-test-job_tmp.yaml

function config
{
    sed -i "" "s/HOST/$HOST/" $CUR_DIR/vector-db-test-job_tmp.yaml
    sed -i "" "s/MQDB_VERSION/$MQDB_VERSION/" $CUR_DIR/vector-db-test-job_tmp.yaml
    sed -i "" "s/TEST_PALTFORM/$TEST_PALTFORM/" $CUR_DIR/vector-db-test-job_tmp.yaml
}

config
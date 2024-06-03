set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
if [ $# -ne 5 ]; then
    echo "wrong parameter";
    exit 1;
fi
VERSION_STRING=$1
GIT_SHORT_COMMIT=$2
GIT_BRANCH=$3
CI_COMMIT_SHA=$4
CI_COMMIT_MESSAGE=${5//[$'\t\r\n']}

cp -f $CUR_DIR/mqdb_server.yaml $CUR_DIR/mqdb_server_tmp.yaml
cp -f $CUR_DIR/vector_db_benchmark.yaml $CUR_DIR/vector_db_benchmark_tmp.yaml

function config
{
    sed -i "s?IMAGE_VERSION?$VERSION_STRING-$GIT_SHORT_COMMIT?" $CUR_DIR/mqdb_server_tmp.yaml
    sed -i "s?VERSION_STRING?$VERSION_STRING?" $CUR_DIR/vector_db_benchmark_tmp.yaml
    sed -i "s?GIT_BRANCH?$GIT_BRANCH?" $CUR_DIR/vector_db_benchmark_tmp.yaml
    sed -i "s?CI_COMMIT_SHA?$CI_COMMIT_SHA?" $CUR_DIR/vector_db_benchmark_tmp.yaml
    sed -i "s?CI_COMMIT_MESSAGE?$CI_COMMIT_MESSAGE?" $CUR_DIR/vector_db_benchmark_tmp.yaml
}

config
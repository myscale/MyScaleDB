#!/usr/bin/env bash
set -e

DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $DIR/init-env.sh myscale_stateful

function clean
{
  # For local run, remove last test file
  echo "***remove last test file***"
  rm -rf $WORKPATH/packages/* ||:;
  rm -rf $WORKPATH/s3downloader ||:;
  rm -rf $WORKPATH/tests/* ||:;
  rm -rf $WORKPATH/test_output/* ||:;
  rm -rf $WORKPATH/clickhouse-test ||:;
  rm -rf /create.sql ||:;
  tree -L 2 $WORKPATH
}

function copy_file
{
  echo "***Copy the file to the relevant directory***"
  clean
  cp -rf $PROJECT_PATH/artifacts/clickhouse-*.deb docker/test/myscale_stateful/packages
  cp -rf $PROJECT_PATH/docker/myscale_test_script/s3downloader docker/test/myscale_stateful/s3downloader
  cp -rf $PROJECT_PATH/tests/queries docker/test/myscale_stateful/tests/
  cp -rf $PROJECT_PATH/tests/performance docker/test/myscale_stateful/tests/
  cp -rf $PROJECT_PATH/tests/config docker/test/myscale_stateful/tests/
  cp -rf $PROJECT_PATH/tests/clickhouse-test docker/test/myscale_stateful/
  cp -rf $PROJECT_PATH/docker/test/myscale_stateful/create.sql /create.sql
  echo "***Test environment initialization completed***"
  tree -L 2 $WORKPATH

  ln -snf $WORKPATH/packages /package_folder
  ln -snf $WORKPATH/clickhouse-test /usr/bin/clickhouse-test
  ln -snf $WORKPATH/s3downloader /s3downloader
  ln -snf $WORKPATH/tests /usr/share/clickhouse-test
  ln -snf $WORKPATH/test_output /test_output
}

function run_test
{
  cd /
  export USE_AZURE_STORAGE_FOR_MERGE_TREE=0;
  MAX_RUN_TIME=9720 ADDITIONAL_OPTIONS="--hung-check --print-time --no-vector-search" \
    DATASETS_URL="https://mqdb-release-1253802058.cos.ap-beijing.myqcloud.com/datasets" \
    DATASETS="hits visits" \
    source $WORKPATH/run.sh
}

if [[ $1 == "clean" ]];
then
  clean
elif [[ $1 == "skip_copy" ]];
then
  echo "***RUN TEST***";
  run_test
else
  copy_file
  run_test
fi

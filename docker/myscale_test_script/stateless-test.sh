#!/usr/bin/env bash
set -e

DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $DIR/init-env.sh myscale_stateless

vector_search_test=${1:-false}

function clean
{
  # For local run, remove last test file
  echo "***remove last test file***"
  rm -rf $WORKPATH/tests/* ||:;
  rm -rf $WORKPATH/packages/* ||:;
  rm -rf $WORKPATH/test_output/* ||:;
  rm -rf $WORKPATH/clickhouse-test ||:;
  rm -rf rm -rf /etc/clickhouse-server/* ||:
  rm -rf rm -rf /var/lib/clickhouse/* ||:
  tree -L 2 $WORKPATH
}

function copy_file
{
  echo "***Copy the file to the relevant directory***"
  clean
  cp -rf $PROJECT_PATH/artifacts/clickhouse-*.deb $WORKPATH/packages/.
  cp -rf $PROJECT_PATH/tests/queries $WORKPATH/tests/
  cp -rf $PROJECT_PATH/tests/performance $WORKPATH/tests/
  cp -rf $PROJECT_PATH/tests/config $WORKPATH/tests/
  cp -rf $PROJECT_PATH/tests/clickhouse-test $WORKPATH/
  echo "***Test environment initialization completed***"
  tree -L 2 $WORKPATH

  ln -snf $WORKPATH/packages /package_folder
  ln -snf $WORKPATH/clickhouse-test /usr/bin/clickhouse-test
  ln -snf $WORKPATH/tests /usr/share/clickhouse-test
  ln -snf $WORKPATH/test_output /test_output
}

function run_test
{
  cd /
  export USE_AZURE_STORAGE_FOR_MERGE_TREE=0;
  export USE_OLD_ANALYZER=0;

  if [[ $vector_search_test == "true" ]];
  then
    OPTIONS="--no-stateless --no-stateful"
    USE_OLD_ANALYZER=1
  else
    OPTIONS="--no-vector-search --no-stateful"
    USE_OLD_ANALYZER=0
  fi

  MAX_RUN_TIME=9720 S3_URL="https://clickhouse-datasets.s3.amazonaws.com" \
  ADDITIONAL_OPTIONS="--hung-check --print-time $OPTIONS --force-retry" \
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

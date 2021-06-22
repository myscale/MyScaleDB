#!/usr/bin/env bash
set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_PATH=$CUR_DIR/../../..
mkdir -p $PROJECT_PATH/performance_pack ||:
copy_func=${1:-all}

function rm_all
{
    echo "***WILL REMOVE ALL FILE***"
    rm -rf $PROJECT_PATH/performance_pack ||:
}

function copy_binary
{
    echo "***COPY clickhouse binary***"
    mkdir -p $PROJECT_PATH/performance_pack ||:
    cp -rf $PROJECT_PATH/build/programs/clickhouse* $PROJECT_PATH/performance_pack/.
}

function set_listen_config
{
    echo "***set performance test listen config***"
    if [ -n $PROJECT_PATH/performance_pack/config/config.d/zzz-perf-comparison-tweaks-config.xml ]; then
        sed -i "/.*listen_host.*/d" `grep -rl ".*listen_host.*" $PROJECT_PATH/performance_pack/config/config.d/*`
    fi;
    rm -rf $PROJECT_PATH/performance_pack/config/config.d/listen.xml ||:
    echo '<clickhouse><listen_host>0.0.0.0</listen_host></clickhouse>' >>$PROJECT_PATH/performance_pack/config/config.d/listen.xml
}

function copy_config
{
    echo "***COPY clickhouse config file***"
    mkdir $PROJECT_PATH/performance_pack/config ||:
    rm -rf $PROJECT_PATH/performance_pack/config/* ||:
    cp -rfd $PROJECT_PATH/programs/server/* $PROJECT_PATH/performance_pack/config/.
    cp -rf $PROJECT_PATH/docker/test/mqdb_run_performance/config/* $PROJECT_PATH/performance_pack/config/.
    set_listen_config
    rm -rf $PROJECT_PATH/performance_pack/top_level_domains ||:
    mkdir $PROJECT_PATH/performance_pack/top_level_domains
    cp -rf $PROJECT_PATH/tests/config/top_level_domains $PROJECT_PATH/performance_pack/.
}

function copy_git_info
{
    echo "***COPY git info***"
    mkdir $PROJECT_PATH/performance_pack/ch ||:
    rm -rf $PROJECT_PATH/performance_pack/ch/*
    rsync -a --exclude='modules/*' $PROJECT_PATH/.git $PROJECT_PATH/performance_pack/ch/.
}

function copy_performance_file
{
    echo "***COPY performance test file***"
    mkdir $PROJECT_PATH/performance_pack/performance ||:
    rm -rf $PROJECT_PATH/performance_pack/performance/* ||:
    cp -rf $PROJECT_PATH/tests/performance/* $PROJECT_PATH/performance_pack/performance/.
}

function copy_scripts
{
    echo "***COPY scripts***"
    # mkdir $PROJECT_PATH/performance_pack/scripts ||:
    # rm -rf $PROJECT_PATH/performance_pack/scripts/* ||:
    # cp -rf $PROJECT_PATH/docker/test/mqdb_run_performance/* $PROJECT_PATH/performance_pack/scripts/.
    rsync -a --exclude={'output/*','packages/*','test_output/*','tests/*','workspace'} $PROJECT_PATH/docker/test/mqdb_run_performance $PROJECT_PATH/performance_pack/.
    mv $PROJECT_PATH/performance_pack/mqdb_run_performance $PROJECT_PATH/performance_pack/scripts
}

if [[ "$copy_func" == "all" ]]; then
    echo "********WILL COPY ALL FILE********"
    rm_all
    copy_binary
    # set_listen_config
    copy_config
    copy_git_info
    copy_performance_file
    copy_scripts
elif [[ "$copy_func" == "help" ]]; then
    echo "uses funcs: 
          'copy_binary'
          'set_listen_config'
          'copy_config'
          'copy_git_info'
          'copy_performance_file'
          'copy_scripts'
          'rm_all'
          ! use 'all' will copy all file !
         "
else
    ${copy_func}
fi

# 压缩 tar -zcvhf performance_pack.tar.gz performance_pack
# 解压 tar zxvf performance_pack.tar.gz
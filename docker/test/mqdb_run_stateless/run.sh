#!/bin/bash

# fail on errors, verbose and export all env variables
set -e -x -a

# Choose random timezone for this test run.
TZ="$(grep -v '#' /usr/share/zoneinfo/zone.tab | awk '{print $3}' | shuf | head -n1)"
echo "Choosen random timezone $TZ"
ln -snf "/usr/share/zoneinfo/$TZ" /etc/localtime && echo "$TZ" >/etc/timezone

source /etc/profile
arch="$(dpkg --print-architecture)"
if [[ "x$arch" = "xamd64" ]]; then
    echo "/opt/intel/oneapi/mkl/$INTEL_ONEAPI_VERSION/lib/intel64" >>/etc/ld.so.conf.d/libc.conf
    ldconfig
fi

dpkg -i package_folder/clickhouse-common-static_*$arch.deb
dpkg -i package_folder/clickhouse-common-static-dbg_*$arch.deb
dpkg -i package_folder/clickhouse-server_*$arch.deb
dpkg -i package_folder/clickhouse-client_*$arch.deb
# dpkg -i package_folder/clickhouse-test_*.deb
chmod a+x /usr/bin/clickhouse-test
# install test configs
/usr/share/clickhouse-test/config/install.sh
rm -rf /etc/clickhouse-server/config.d/listen.xml
echo '<clickhouse><listen_host>0.0.0.0</listen_host></clickhouse>' >>/etc/clickhouse-server/config.d/listen.xml
echo '<clickhouse><interserver_listen_host>0.0.0.0</interserver_listen_host></clickhouse>' >>/etc/clickhouse-server/config.d/interserver_listen_host.xml
# cp -r clickhouse-test /usr/bin/clickhouse-test

if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
    echo "Azure is disabled"
else
    azurite-blob --blobHost 0.0.0.0 --blobPort 10000 --debug /azurite_log &
fi

./setup_minio.sh stateless
./setup_hdfs_minicluster.sh

# For flaky check we also enable thread fuzzer
if [ "$NUM_TRIES" -gt "1" ]; then
    export THREAD_FUZZER_CPU_TIME_PERIOD_US=1000
    export THREAD_FUZZER_SLEEP_PROBABILITY=0.1
    export THREAD_FUZZER_SLEEP_TIME_US=100000

    export THREAD_FUZZER_pthread_mutex_lock_BEFORE_MIGRATE_PROBABILITY=1
    export THREAD_FUZZER_pthread_mutex_lock_AFTER_MIGRATE_PROBABILITY=1
    export THREAD_FUZZER_pthread_mutex_unlock_BEFORE_MIGRATE_PROBABILITY=1
    export THREAD_FUZZER_pthread_mutex_unlock_AFTER_MIGRATE_PROBABILITY=1

    export THREAD_FUZZER_pthread_mutex_lock_BEFORE_SLEEP_PROBABILITY=0.001
    export THREAD_FUZZER_pthread_mutex_lock_AFTER_SLEEP_PROBABILITY=0.001
    export THREAD_FUZZER_pthread_mutex_unlock_BEFORE_SLEEP_PROBABILITY=0.001
    export THREAD_FUZZER_pthread_mutex_unlock_AFTER_SLEEP_PROBABILITY=0.001
    export THREAD_FUZZER_pthread_mutex_lock_BEFORE_SLEEP_TIME_US=10000
    export THREAD_FUZZER_pthread_mutex_lock_AFTER_SLEEP_TIME_US=10000
    export THREAD_FUZZER_pthread_mutex_unlock_BEFORE_SLEEP_TIME_US=10000
    export THREAD_FUZZER_pthread_mutex_unlock_AFTER_SLEEP_TIME_US=10000

    # simpliest way to forward env variables to server
    sudo -E -u clickhouse /usr/bin/clickhouse-server --config /etc/clickhouse-server/config.xml --daemon
else
    sudo clickhouse start
fi

if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then

    sudo -E -u clickhouse /usr/bin/clickhouse server --config /etc/clickhouse-server1/config.xml --daemon \
        -- --path /var/lib/clickhouse1/ --logger.stderr /var/log/clickhouse-server/stderr1.log \
        --logger.log /var/log/clickhouse-server/clickhouse-server1.log --logger.errorlog /var/log/clickhouse-server/clickhouse-server1.err.log \
        --tcp_port 19000 --tcp_port_secure 19440 --http_port 18123 --https_port 18443 --interserver_http_port 19009 --tcp_with_proxy_port 19010 \
        --mysql_port 19004 --postgresql_port 19005 \
        --keeper_server.tcp_port 19181 --keeper_server.server_id 2 \
        --macros.replica r2 # It doesn't work :(

    sudo -E -u clickhouse /usr/bin/clickhouse server --config /etc/clickhouse-server2/config.xml --daemon \
        -- --path /var/lib/clickhouse2/ --logger.stderr /var/log/clickhouse-server/stderr2.log \
        --logger.log /var/log/clickhouse-server/clickhouse-server2.log --logger.errorlog /var/log/clickhouse-server/clickhouse-server2.err.log \
        --tcp_port 29000 --tcp_port_secure 29440 --http_port 28123 --https_port 28443 --interserver_http_port 29009 --tcp_with_proxy_port 29010 \
        --mysql_port 29004 --postgresql_port 29005 \
        --keeper_server.tcp_port 29181 --keeper_server.server_id 3 \
        --macros.shard s2 # It doesn't work :(

    MAX_RUN_TIME=$((MAX_RUN_TIME < 9000 ? MAX_RUN_TIME : 9000)) # min(MAX_RUN_TIME, 2.5 hours)
    MAX_RUN_TIME=$((MAX_RUN_TIME != 0 ? MAX_RUN_TIME : 9000))   # set to 2.5 hours if 0 (unlimited)
fi

function wait_server_setup() {
    counter=0
    until clickhouse-client --query "SELECT 1"; do
        if [ "$counter" -gt 120 ]; then
            echo "Cannot start clickhouse-server"
            cat /var/log/clickhouse-server/stdout.log ||:
            tail -n1000 /var/log/clickhouse-server/stderr.log ||:
            tail -n1000 /var/log/clickhouse-server/clickhouse-server.log ||:
            break
        fi
        sleep 0.5
        counter=$((counter + 1))
    done
}

wait_server_setup

lsof -i

function run_tests() {
    set -x
    # We can have several additional options so we path them as array because it's
    # more idiologically correct.
    read -ra ADDITIONAL_OPTIONS <<<"${ADDITIONAL_OPTIONS:-}"

    # Skip these tests, because they fail when we rerun them multiple times
    if [ "$NUM_TRIES" -gt "1" ]; then
        ADDITIONAL_OPTIONS+=('--order=random')
        ADDITIONAL_OPTIONS+=('--skip')
        ADDITIONAL_OPTIONS+=('00000_no_tests_to_skip')
        # Note that flaky check must be ran in parallel, but for now we run
        # everything in parallel except DatabaseReplicated. See below.
    fi

    if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
        ADDITIONAL_OPTIONS+=('--replicated-database')
        ADDITIONAL_OPTIONS+=('--jobs')
        ADDITIONAL_OPTIONS+=('2')
    else
        # Too many tests fail for DatabaseReplicated in parallel. All other
        # configurations are OK.
        ADDITIONAL_OPTIONS+=('--jobs')
        ADDITIONAL_OPTIONS+=('8')
    fi

    set +e
    clickhouse-test --testname --shard --zookeeper --check-zookeeper-session --hung-check --print-time \
        --test-runs "$NUM_TRIES" "${ADDITIONAL_OPTIONS[@]} " \
        --skip 00000_prepare \
        01086_odbc_roundtrip 02049_clickhouse_local_merge_tree 01753_max_uri_size 01753_max_uri_size \
        01565_reconnect_after_client_error 01324_settings_documentation 01293_show_clusters \
        02344_show_caches 02456_BLAKE3_hash_function_test \
        01271_show_privileges 01158_zookeeper_log_long 01528_clickhouse_local_prepare_parts \
        01658_read_file_to_stringcolumn 01600_detach_permanently 01527_clickhouse_local_optimize \
        02047 01039 00993 02207 02117 02226 01606_git_import 01945_show_debug_warning 02420_stracktrace_debug_symbols \
        02435_rollback_cancelled_queries 02345_implicit_transaction 01193_metadata_loading 01880_remote_ipv6 \
        01103_check_cpu_instructions_at_startup 02125_many_mutations 02161_addressToLineWithInlines \
        01680_date_time_add_ubsan 2>&1 |
        ts '%Y-%m-%d %H:%M:%S' |
        tee -a test_output/test_result.txt
    set -e
}

export -f run_tests

timeout "$MAX_RUN_TIME" bash -c run_tests || :

echo "Files in current directory"
ls -la ./
echo "Files in root directory"
ls -la /

python3 process_functional_tests_result.py || echo -e "failure\tCannot parse results" >/test_output/check_status.tsv

clickhouse-client -q "system flush logs" || :

# Stop server so we can safely read data with clickhouse-local.
# Why do we read data with clickhouse-local?
# Because it's the simplest way to read it when server has crashed.
sudo clickhouse stop ||:
if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
    sudo clickhouse stop --pid-path /var/run/clickhouse-server1 ||:
    sudo clickhouse stop --pid-path /var/run/clickhouse-server2 ||:
fi

rg -Fa "<Fatal>" /var/log/clickhouse-server/clickhouse-server.log ||:
rg -A50 -Fa "============" /var/log/clickhouse-server/stderr.log ||:
zstd --threads=0 < /var/log/clickhouse-server/clickhouse-server.log > /test_output/clickhouse-server.log.zst &

# Compress tables.
#
# NOTE:
# - that due to tests with s3 storage we cannot use /var/lib/clickhouse/data
#   directly
# - even though ci auto-compress some files (but not *.tsv) it does this only
#   for files >64MB, we want this files to be compressed explicitly
for table in query_log zookeeper_log trace_log transactions_info_log
do
    clickhouse-local --path /var/lib/clickhouse/ --only-system-tables -q "select * from system.$table format TSVWithNamesAndTypes" | zstd --threads=0 > /test_output/$table.tsv.zst ||:
    if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
        clickhouse-local --path /var/lib/clickhouse1/ --only-system-tables -q "select * from system.$table format TSVWithNamesAndTypes" | zstd --threads=0 > /test_output/$table.1.tsv.zst ||:
        clickhouse-local --path /var/lib/clickhouse2/ --only-system-tables -q "select * from system.$table format TSVWithNamesAndTypes" | zstd --threads=0 > /test_output/$table.2.tsv.zst ||:
    fi
done

# Also export trace log in flamegraph-friendly format.
for trace_type in CPU Memory Real
do
    clickhouse-local --path /var/lib/clickhouse/ --only-system-tables -q "
            select
                arrayStringConcat((arrayMap(x -> concat(splitByChar('/', addressToLine(x))[-1], '#', demangle(addressToSymbol(x)) ), trace)), ';') AS stack,
                count(*) AS samples
            from system.trace_log
            where trace_type = '$trace_type'
            group by trace
            order by samples desc
            settings allow_introspection_functions = 1
            format TabSeparated" \
        | zstd --threads=0 > "/test_output/trace-log-$trace_type-flamegraph.tsv.zst" ||:
done

# Compressed (FIXME: remove once only github actions will be left)
rm /var/log/clickhouse-server/clickhouse-server.log
mv /var/log/clickhouse-server/stderr.log /test_output/ ||:
if [[ -n "$WITH_COVERAGE" ]] && [[ "$WITH_COVERAGE" -eq 1 ]]; then
    tar --zstd -chf /test_output/clickhouse_coverage.tar.zst /profraw ||:
fi

tar -chf /test_output/coordination.tar /var/lib/clickhouse/coordination ||:

if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
    rg -Fa "<Fatal>" /var/log/clickhouse-server/clickhouse-server1.log ||:
    rg -Fa "<Fatal>" /var/log/clickhouse-server/clickhouse-server2.log ||:
    zstd --threads=0 < /var/log/clickhouse-server/clickhouse-server1.log > /test_output/clickhouse-server1.log.zst ||:
    zstd --threads=0 < /var/log/clickhouse-server/clickhouse-server2.log > /test_output/clickhouse-server2.log.zst ||:
    # FIXME: remove once only github actions will be left
    rm /var/log/clickhouse-server/clickhouse-server1.log
    rm /var/log/clickhouse-server/clickhouse-server2.log
    mv /var/log/clickhouse-server/stderr1.log /test_output/ ||:
    mv /var/log/clickhouse-server/stderr2.log /test_output/ ||:
    tar -chf /test_output/coordination1.tar /var/lib/clickhouse1/coordination ||:
    tar -chf /test_output/coordination2.tar /var/lib/clickhouse2/coordination ||:
fi

#!/bin/bash
# shellcheck disable=SC2094
# shellcheck disable=SC2086
# shellcheck disable=SC2024

set -x

# Thread Fuzzer allows to check more permutations of possible thread scheduling
# and find more potential issues.

PROJECT_PATH=${1:-/workspace/ClickHouse}
SHA_TO_TEST=${2:-run_for_test}
WORKPATH=$PROJECT_PATH/docker/test/mqdb_run_stress

# mkdir -p $WORKPATH/workspace/
# cd $WORKPATH/workspace
cp /mc . && cp /minio . ||:

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

arch="$(dpkg --print-architecture)"
if [[ "x$arch" = "xamd64" ]]; then
    echo "/opt/intel/oneapi/mkl/$INTEL_ONEAPI_VERSION/lib/intel64" >>/etc/ld.so.conf.d/libc.conf
    ldconfig
fi


dpkg -i $WORKPATH/packages/clickhouse-common-static_*$arch.deb
dpkg -i $WORKPATH/packages/clickhouse-common-static-dbg_*$arch.deb
dpkg -i $WORKPATH/packages/clickhouse-server_*$arch.deb
dpkg -i $WORKPATH/packages/clickhouse-client_*$arch.deb

rm -rf /usr/bin/clickhouse-test
cp $WORKPATH/clickhouse-test /usr/bin/clickhouse-test
chmod a+x /usr/bin/clickhouse-test

rm -rf /usr/share/clickhouse-test
ls $WORKPATH/tests
cp -rf $WORKPATH/tests /usr/share/clickhouse-test

function configure()
{
    # install test configs
    /usr/share/clickhouse-test/config/install.sh
    rm -rf /etc/clickhouse-server/config.d/listen.xml
    echo '<clickhouse><listen_host>0.0.0.0</listen_host></clickhouse>' >>/etc/clickhouse-server/config.d/listen.xml

    # we mount tests folder from repo to /usr/share
    ln -s /usr/share/clickhouse-test/clickhouse-test /usr/bin/clickhouse-test

    # avoid too slow startup
    sudo cat /etc/clickhouse-server/config.d/keeper_port.xml | sed "s|<snapshot_distance>100000</snapshot_distance>|<snapshot_distance>10000</snapshot_distance>|" > /etc/clickhouse-server/config.d/keeper_port.xml.tmp
    sudo mv /etc/clickhouse-server/config.d/keeper_port.xml.tmp /etc/clickhouse-server/config.d/keeper_port.xml
    sudo chown clickhouse /etc/clickhouse-server/config.d/keeper_port.xml
    sudo chgrp clickhouse /etc/clickhouse-server/config.d/keeper_port.xml

    # for clickhouse-server (via service)
    echo "ASAN_OPTIONS='malloc_context_size=10 verbosity=1 allocator_release_to_os_interval_ms=10000'" >> /etc/environment
    # for clickhouse-client
    export ASAN_OPTIONS='malloc_context_size=10 allocator_release_to_os_interval_ms=10000'

    # since we run clickhouse from root
    sudo chown root: /var/lib/clickhouse

    # Set more frequent update period of asynchronous metrics to more frequently update information about real memory usage (less chance of OOM).
    echo "<clickhouse><asynchronous_metrics_update_period_s>1</asynchronous_metrics_update_period_s></clickhouse>" \
        > /etc/clickhouse-server/config.d/asynchronous_metrics_update_period_s.xml

    local total_mem
    total_mem=$(awk '/MemTotal/ { print $(NF-1) }' /proc/meminfo) # KiB
    total_mem=$(( total_mem*1024 )) # bytes
    # Set maximum memory usage as half of total memory (less chance of OOM).
    #
    # But not via max_server_memory_usage but via max_memory_usage_for_user,
    # so that we can override this setting and execute service queries, like:
    # - hung check
    # - show/drop database
    # - ...
    #
    # So max_memory_usage_for_user will be a soft limit, and
    # max_server_memory_usage will be hard limit, and queries that should be
    # executed regardless memory limits will use max_memory_usage_for_user=0,
    # instead of relying on max_untracked_memory
    local max_server_mem
    max_server_mem=$((total_mem*75/100)) # 75%
    echo "Setting max_server_memory_usage=$max_server_mem"
    cat > /etc/clickhouse-server/config.d/max_server_memory_usage.xml <<EOL
<clickhouse>
    <max_server_memory_usage>${max_server_mem}</max_server_memory_usage>
</clickhouse>
EOL
    local max_users_mem
    max_users_mem=$((total_mem*50/100)) # 50%
    echo "Setting max_memory_usage_for_user=$max_users_mem"
    cat > /etc/clickhouse-server/users.d/max_memory_usage_for_user.xml <<EOL
<clickhouse>
    <profiles>
        <default>
            <max_memory_usage_for_user>${max_users_mem}</max_memory_usage_for_user>
        </default>
    </profiles>
</clickhouse>
EOL
}

function stop()
{
    clickhouse stop
}

function start()
{
    # Rename existing log file - it will be more convenient to read separate files for separate server runs.
    if [ -f '/var/log/clickhouse-server/clickhouse-server.log' ]
    then
        log_file_counter=1
        while [ -f "/var/log/clickhouse-server/clickhouse-server.log.${log_file_counter}" ]
        do
            log_file_counter=$((log_file_counter + 1))
        done
        mv '/var/log/clickhouse-server/clickhouse-server.log' "/var/log/clickhouse-server/clickhouse-server.log.${log_file_counter}"
    fi

    counter=0
    until clickhouse-client --query "SELECT 1"
    do
        if [ "$counter" -gt 240 ]
        then
            echo "Cannot start clickhouse-server"
            cat /var/log/clickhouse-server/stdout.log
            tail -n1000 /var/log/clickhouse-server/stderr.log
            tail -n100000 /var/log/clickhouse-server/clickhouse-server.log | grep -F -v -e '<Warning> RaftInstance:' -e '<Information> RaftInstance' | tail -n1000
            break
        fi
        # use root to match with current uid
        clickhouse start --user root >/var/log/clickhouse-server/stdout.log 2>>/var/log/clickhouse-server/stderr.log
        sleep 0.5
        counter=$((counter + 1))
    done

    # Set follow-fork-mode to parent, because we attach to clickhouse-server, not to watchdog
    # and clickhouse-server can do fork-exec, for example, to run some bridge.
    # Do not set nostop noprint for all signals, because some it may cause gdb to hang,
    # explicitly ignore non-fatal signals that are used by server.
    # Number of SIGRTMIN can be determined only in runtime.
    RTMIN=$(kill -l SIGRTMIN)
    echo "
set follow-fork-mode parent
handle SIGHUP nostop noprint pass
handle SIGINT nostop noprint pass
handle SIGQUIT nostop noprint pass
handle SIGPIPE nostop noprint pass
handle SIGTERM nostop noprint pass
handle SIGUSR1 nostop noprint pass
handle SIGUSR2 nostop noprint pass
handle SIG$RTMIN nostop noprint pass
info signals
continue
gcore
backtrace full
thread apply all backtrace full
info registers
disassemble /s
up
disassemble /s
up
disassemble /s
p \"done\"
detach
quit
" > $WORKPATH/test_output/script.gdb

    # FIXME Hung check may work incorrectly because of attached gdb
    # 1. False positives are possible
    # 2. We cannot attach another gdb to get stacktraces if some queries hung
    gdb -batch -command $WORKPATH/test_output/script.gdb -p "$(cat /var/run/clickhouse-server/clickhouse-server.pid)" | ts '%Y-%m-%d %H:%M:%S' >> $WORKPATH/test_output/gdb.log &
    sleep 5
    # gdb will send SIGSTOP, spend some time loading debug info and then send SIGCONT, wait for it (up to send_timeout, 300s)
    time clickhouse-client --query "SELECT 'Connected to clickhouse-server after attaching gdb'" ||:
}
# [BUG] skip check clickhouse-server when attach database
function skip_attach_log_check()
{
    # tar -czvf ck_attack_database_log.tar.gz /var/log/clickhouse
    # mv ck_attack_database_log.tar.gz $WORKPATH/test_output
    mv /var/log/clickhouse-server/clickhouse-server.log $WORKPATH/test_output/attach_database.log
    rm -rf /var/log/clickhouse-server/clickhouse-server.log
}

configure

/setup_minio.sh

start

# shellcheck disable=SC2086 # No quotes because I want to split it into words.
$PROJECT_PATH/docker/test/mqdb_test_script/s3downloader --url-prefix "$DATASETS_URL" --dataset-names $DATASETS
chmod 777 -R /var/lib/clickhouse
clickhouse-client --query "ATTACH DATABASE IF NOT EXISTS datasets ENGINE = Ordinary"
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS test"
sleep 30
stop

skip_attach_log_check

start

clickhouse-client --query "SHOW TABLES FROM datasets"
clickhouse-client --query "SHOW TABLES FROM test"
clickhouse-client --query "RENAME TABLE datasets.hits_v1 TO test.hits"
clickhouse-client --query "RENAME TABLE datasets.visits_v1 TO test.visits"
# s3_cache is not currently supported
# clickhouse-client --query "CREATE TABLE test.hits_s3  (WatchID UInt64, JavaEnable UInt8, Title String, GoodEvent Int16, EventTime DateTime, EventDate Date, CounterID UInt32, ClientIP UInt32, ClientIP6 FixedString(16), RegionID UInt32, UserID UInt64, CounterClass Int8, OS UInt8, UserAgent UInt8, URL String, Referer String, URLDomain String, RefererDomain String, Refresh UInt8, IsRobot UInt8, RefererCategories Array(UInt16), URLCategories Array(UInt16), URLRegions Array(UInt32), RefererRegions Array(UInt32), ResolutionWidth UInt16, ResolutionHeight UInt16, ResolutionDepth UInt8, FlashMajor UInt8, FlashMinor UInt8, FlashMinor2 String, NetMajor UInt8, NetMinor UInt8, UserAgentMajor UInt16, UserAgentMinor FixedString(2), CookieEnable UInt8, JavascriptEnable UInt8, IsMobile UInt8, MobilePhone UInt8, MobilePhoneModel String, Params String, IPNetworkID UInt32, TraficSourceID Int8, SearchEngineID UInt16, SearchPhrase String, AdvEngineID UInt8, IsArtifical UInt8, WindowClientWidth UInt16, WindowClientHeight UInt16, ClientTimeZone Int16, ClientEventTime DateTime, SilverlightVersion1 UInt8, SilverlightVersion2 UInt8, SilverlightVersion3 UInt32, SilverlightVersion4 UInt16, PageCharset String, CodeVersion UInt32, IsLink UInt8, IsDownload UInt8, IsNotBounce UInt8, FUniqID UInt64, HID UInt32, IsOldCounter UInt8, IsEvent UInt8, IsParameter UInt8, DontCountHits UInt8, WithHash UInt8, HitColor FixedString(1), UTCEventTime DateTime, Age UInt8, Sex UInt8, Income UInt8, Interests UInt16, Robotness UInt8, GeneralInterests Array(UInt16), RemoteIP UInt32, RemoteIP6 FixedString(16), WindowName Int32, OpenerName Int32, HistoryLength Int16, BrowserLanguage FixedString(2), BrowserCountry FixedString(2), SocialNetwork String, SocialAction String, HTTPError UInt16, SendTiming Int32, DNSTiming Int32, ConnectTiming Int32, ResponseStartTiming Int32, ResponseEndTiming Int32, FetchTiming Int32, RedirectTiming Int32, DOMInteractiveTiming Int32, DOMContentLoadedTiming Int32, DOMCompleteTiming Int32, LoadEventStartTiming Int32, LoadEventEndTiming Int32, NSToDOMContentLoadedTiming Int32, FirstPaintTiming Int32, RedirectCount Int8, SocialSourceNetworkID UInt8, SocialSourcePage String, ParamPrice Int64, ParamOrderID String, ParamCurrency FixedString(3), ParamCurrencyID UInt16, GoalsReached Array(UInt32), OpenstatServiceName String, OpenstatCampaignID String, OpenstatAdID String, OpenstatSourceID String, UTMSource String, UTMMedium String, UTMCampaign String, UTMContent String, UTMTerm String, FromTag String, HasGCLID UInt8, RefererHash UInt64, URLHash UInt64, CLID UInt32, YCLID UInt64, ShareService String, ShareURL String, ShareTitle String, ParsedParams Nested(Key1 String, Key2 String, Key3 String, Key4 String, Key5 String, ValueDouble Float64), IslandID FixedString(16), RequestNum UInt32, RequestTry UInt8) ENGINE = MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity = 8192, storage_policy='s3_cache'"
# clickhouse-client --query "INSERT INTO test.hits_s3 SELECT * FROM test.hits"

clickhouse-client --query "SHOW TABLES FROM test"

$WORKPATH/stress --hung-check --drop-databases --output-folder $WORKPATH/test_output --skip-func-tests "$SKIP_TESTS_OPTION" \
    && echo -e 'Test script exit code\tOK' >> $WORKPATH/test_output/test_results.tsv \
    || echo -e 'Test script failed\tFAIL' >> $WORKPATH/test_output/test_results.tsv

stop
start

clickhouse-client --query "SELECT 'Server successfully started', 'OK'" >> $WORKPATH/test_output/test_results.tsv \
                       || echo -e 'Server failed to start\tFAIL' >> $WORKPATH/test_output/test_results.tsv

[ -f /var/log/clickhouse-server/clickhouse-server.log ] || echo -e "Server log does not exist\tFAIL"
[ -f /var/log/clickhouse-server/stderr.log ] || echo -e "Stderr log does not exist\tFAIL"

# Print Fatal log messages to stdout
zgrep -Fa " <Fatal> " /var/log/clickhouse-server/clickhouse-server.log*

# Grep logs for sanitizer asserts, crashes and other critical errors

# Sanitizer asserts
grep -Fa "==================" /var/log/clickhouse-server/stderr.log | grep -v "in query:" >> $WORKPATH/test_output/tmp
grep -Fa "WARNING" /var/log/clickhouse-server/stderr.log >> $WORKPATH/test_output/tmp
zgrep -Fav "ASan doesn't fully support makecontext/swapcontext functions" $WORKPATH/test_output/tmp > /dev/null \
    && echo -e 'Sanitizer assert (in stderr.log)\tFAIL' >> $WORKPATH/test_output/test_results.tsv \
    || echo -e 'No sanitizer asserts\tOK' >> $WORKPATH/test_output/test_results.tsv
rm -f $WORKPATH/test_output/tmp

# OOM
zgrep -Fa " <Fatal> Application: Child process was terminated by signal 9" /var/log/clickhouse-server/clickhouse-server.log* > /dev/null \
    && echo -e 'OOM killer (or signal 9) in clickhouse-server.log\tFAIL' >> $WORKPATH/test_output/test_results.tsv \
    || echo -e 'No OOM messages in clickhouse-server.log\tOK' >> $WORKPATH/test_output/test_results.tsv

# Logical errors
zgrep -Fa "Code: 49, e.displayText() = DB::Exception:" /var/log/clickhouse-server/clickhouse-server.log* > /dev/null \
    && echo -e 'Logical error thrown (see clickhouse-server.log)\tFAIL' >> $WORKPATH/test_output/test_results.tsv \
    || echo -e 'No logical errors\tOK' >> $WORKPATH/test_output/test_results.tsv

# Crash
zgrep -Fa "########################################" /var/log/clickhouse-server/clickhouse-server.log* > /dev/null \
    && echo -e 'Killed by signal (in clickhouse-server.log)\tFAIL' >> $WORKPATH/test_output/test_results.tsv \
    || echo -e 'Not crashed\tOK' >> $WORKPATH/test_output/test_results.tsv

# It also checks for crash without stacktrace (printed by watchdog)
zgrep -Fa " <Fatal> " /var/log/clickhouse-server/clickhouse-server.log* > /dev/null \
    && echo -e 'Fatal message in clickhouse-server.log\tFAIL' >> $WORKPATH/test_output/test_results.tsv \
    || echo -e 'No fatal messages in clickhouse-server.log\tOK' >> $WORKPATH/test_output/test_results.tsv

zgrep -Fa "########################################" $WORKPATH/test_output/* > /dev/null \
    && echo -e 'Killed by signal (output files)\tFAIL' >> $WORKPATH/test_output/test_results.tsv

zgrep -Fa " received signal " $WORKPATH/test_output/gdb.log > /dev/null \
    && echo -e 'Found signal in gdb.log\tFAIL' >> $WORKPATH/test_output/test_results.tsv

# Put logs into $WORKPATH/test_output/
for log_file in /var/log/clickhouse-server/clickhouse-server.log*
do
    pigz < "${log_file}" > $WORKPATH/test_output/"$(basename ${log_file})".gz
    # FIXME: remove once only github actions will be left
    rm "${log_file}"
done

tar -chf $WORKPATH/test_output/coordination.tar /var/lib/clickhouse/coordination ||:
mv /var/log/clickhouse-server/stderr.log $WORKPATH/test_output/

# Replace the engine with Ordinary to avoid extra symlinks stuff in artifacts.
# (so that clickhouse-local --path can read it w/o extra care).
sed -i -e "s/ATTACH DATABASE _ UUID '[^']*'/ATTACH DATABASE system/" -e "s/Atomic/Ordinary/" /var/lib/clickhouse/metadata/system.sql
for table in query_log trace_log; do
    sed -i "s/ATTACH TABLE _ UUID '[^']*'/ATTACH TABLE $table/" /var/lib/clickhouse/metadata/system/${table}.sql
    tar -chf $WORKPATH/test_output/${table}_dump.tar /var/lib/clickhouse/metadata/system.sql /var/lib/clickhouse/metadata/system/${table}.sql /var/lib/clickhouse/data/system/${table} ||:
done

# Write check result into check_status.tsv
clickhouse-local --structure "test String, res String" -q "SELECT 'failure', test FROM table WHERE res != 'OK' order by (lower(test) like '%hung%') LIMIT 1" < $WORKPATH/test_output/test_results.tsv > $WORKPATH/test_output/check_status.tsv
[ -s $WORKPATH/test_output/check_status.tsv ] || echo -e "success\tNo errors found" > $WORKPATH/test_output/check_status.tsv

# Core dumps (see gcore)
# Default filename is 'core.PROCESS_ID'
for core in core.*; do
    pigz $core ||:
    mv $core.gz $WORKPATH/test_output/ ||:
done

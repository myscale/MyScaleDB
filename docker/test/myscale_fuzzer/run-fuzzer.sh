#!/bin/bash
# shellcheck disable=SC2086,SC2001,SC2046,SC2030,SC2031,SC2010,SC2015

# shellcheck disable=SC1091
source /setup_export_logs.sh
set -x

# core.COMM.PID-TID
sysctl kernel.core_pattern='core.%e.%p-%P'
dmesg --clear ||:

set -e
set -u
set -o pipefail

PR_TO_TEST=0

TEST_FLOAD=$WORKPATH
cd $TEST_FLOAD

stage=${stage:-clone}

repo_dir=$TEST_FLOAD/ch
PR_TO_TEST=0
ls -al $TEST_FLOAD

# source /etc/profile
# arch="$(dpkg --print-architecture)"
arch="amd64"

mkdir -p $WORKPATH/workspace
cd $WORKPATH/workspace

function clone
{
    rm -rf "$repo_dir" ||:
    mkdir "$repo_dir" ||:

    cp -r $TEST_FLOAD/tests/server $repo_dir
    cp -r $TEST_FLOAD/tests/tests $repo_dir

    ls -lath ||:

}

function download
{
    dpkg -i $WORKPATH/packages/clickhouse-common-static_*$arch.deb
    dpkg -i $WORKPATH/packages/clickhouse-common-static-dbg_*$arch.deb
    dpkg -i $WORKPATH/packages/clickhouse-server_*$arch.deb
    dpkg -i $WORKPATH/packages/clickhouse-client_*$arch.deb
}

function configure
{
    rm -rf $WORKPATH/db ||:
    mkdir $WORKPATH/db ||:
    
    cp -av --dereference "$repo_dir"/server/config* $WORKPATH/db
    cp -av --dereference "$repo_dir"/server/user* $WORKPATH/db
    # TODO figure out which ones are needed
    cp -av --dereference "$TEST_FLOAD"/query-fuzzer-tweaks-users.xml $WORKPATH/db/users.d
    # cp -av --dereference "$PROJECT_PATH"/tests/config/config.d/listen.xml $WORKPATH/db/config.d
    rm -rf $WORKPATH/db/config.d/listen.xml ||:
    rm -rf $WORKPATH/db/config.d/interserver_listen_host.xml ||:
    echo '<clickhouse><listen_host>0.0.0.0</listen_host></clickhouse>' >>$WORKPATH/db/config.d/listen.xml
    echo '<clickhouse><interserver_listen_host>0.0.0.0</interserver_listen_host></clickhouse>' >>$WORKPATH/db/config.d/interserver_listen_host.xml
    cp -av --dereference "$TEST_FLOAD"/allow-nullable-key.xml $WORKPATH/db/config.d
    cat > $WORKPATH/db/config.d/max_server_memory_usage_to_ram_ratio.xml <<EOL
<clickhouse>
    <max_server_memory_usage_to_ram_ratio>0.75</max_server_memory_usage_to_ram_ratio>
</clickhouse>
EOL

    cat > $WORKPATH/db/config.d/core.xml <<EOL
<clickhouse>
    <core_dump>
        <!-- 100GiB -->
        <size_limit>107374182400</size_limit>
    </core_dump>
    <!-- NOTE: no need to configure core_path,
         since clickhouse is not started as daemon (via clickhouse start)
    -->
    <core_path>$PWD</core_path>
</clickhouse>
EOL

    config_logs_export_cluster $WORKPATH/db/config.d/system_logs_export.yaml
}

function filter_exists_and_template
{
    local path
    for path in "$@"; do
        if [ -e "$path" ]; then
            # SC2001 shellcheck suggests:
            # echo ${path//.sql.j2/.gen.sql}
            # but it doesn't allow to use regex
            echo "$path" | sed 's/\.sql\.j2$/.gen.sql/'
        else
            echo "'$path' does not exists" >&2
        fi
    done
}

function stop_server
{
    clickhouse-client --query "select elapsed, query from system.processes" ||:
    clickhouse stop

    # Debug.
    date
    sleep 10
    jobs
    pstree -aspgT
}

function fuzz
{
    $TEST_FLOAD/generate-test-j2.py --path $WORKPATH/ch/tests/queries/0_stateless

    # Obtain the list of newly added tests. They will be fuzzed in more extreme way than other tests.
    # Don't overwrite the NEW_TESTS_OPT so that it can be set from the environment.
    # in MQDB test, NEW_TESTS_OPT parameter not used
    NEW_TESTS_OPT="${NEW_TESTS_OPT:-}"

    mkdir -p /var/run/clickhouse-server

    # NOTE: we use process substitution here to preserve keep $! as a pid of clickhouse-server
    clickhouse-server \
        --config-file $WORKPATH/db/config.xml \
        --pid-file /var/run/clickhouse-server/clickhouse-server.pid \
        -- --path $WORKPATH/db \
           --logger.console=0 \
           --logger.log=server.log 2>&1 | tee -a stderr.log >> server.log 2>&1 &
    for _ in {1..30}
    do
        if clickhouse-client --query "select 1"
        then
            break
        fi
        sleep 1
    done
    server_pid=$(cat /var/run/clickhouse-server/clickhouse-server.pid)

    kill -0 $server_pid

    IS_ASAN=$(clickhouse-client --query "SELECT count() FROM system.build_options WHERE name = 'CXX_FLAGS' AND position('sanitize=address' IN value)")
    if [[ "$IS_ASAN" = "1" ]];
    then
        echo "ASAN build detected. Not using gdb since it disables LeakSanitizer detections"
    else
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
    " > script.gdb

        gdb -batch -command script.gdb -p $server_pid &
        sleep 5
        # gdb will send SIGSTOP, spend some time loading debug info, and then send SIGCONT, wait for it (up to send_timeout, 300s)
        time clickhouse-client --query "SELECT 'Connected to clickhouse-server after attaching gdb'" ||:

        # Check connectivity after we attach gdb, because it might cause the server
        # to freeze, and the fuzzer will fail. In debug build, it can take a lot of time.
        for _ in {1..180}
        do
            if clickhouse-client --query "select 1"
            then
                break
            fi
            sleep 1
        done
        kill -0 $server_pid # This checks that it is our server that is started and not some other one
    fi

    echo 'Server started and responded.'

    setup_logs_replication

    # SC2012: Use find instead of ls to better handle non-alphanumeric filenames. They are all alphanumeric.
    # SC2046: Quote this to prevent word splitting. Actually I need word splitting.
    # shellcheck disable=SC2012,SC2046
    # add vector test for fuzz test
    timeout -s TERM --preserve-status 30m clickhouse-client \
        --receive_timeout=10 \
        --receive_data_timeout_ms=10000 \
        --stacktrace \
        --query-fuzzer-runs=1000 \
        --create-query-fuzzer-runs=50 \
        --queries-file $(ls -1 $WORKPATH/ch/tests/queries/0_stateless/*.sql $WORKPATH/ch/tests/queries/2_vector_search/*.sql | sort -R) \
        $NEW_TESTS_OPT \
        > >(tail -n 100000 > fuzzer.log) \
        2>&1 &
    fuzzer_pid=$!
    echo "Fuzzer pid is $fuzzer_pid"

    # The fuzzer_pid belongs to the timeout process.
    actual_fuzzer_pid=$(ps -o pid= --ppid "$fuzzer_pid")

    if [[ "$IS_ASAN" = "1" ]];
    then
        echo "ASAN build detected. Not using gdb since it disables LeakSanitizer detections"
    else
        echo "Attaching gdb to the fuzzer itself"
        gdb -batch -command script.gdb -p $actual_fuzzer_pid &
    fi

    # Wait for the fuzzer to complete.
    # Note that the 'wait || ...' thing is required so that the script doesn't
    # exit because of 'set -e' when 'wait' returns nonzero code.
    fuzzer_exit_code=0
    wait "$fuzzer_pid" || fuzzer_exit_code=$?
    echo "Fuzzer exit code is $fuzzer_exit_code"

    # If the server dies, most often the fuzzer returns code 210: connetion
    # refused, and sometimes also code 32: attempt to read after eof. For
    # simplicity, check again whether the server is accepting connections, using
    # clickhouse-client. We don't check for existence of server process, because
    # the process is still present while the server is terminating and not
    # accepting the connections anymore.

    for _ in {1..100}
    do
        if clickhouse-client --query "SELECT 1" 2> err
        then
            server_died=0
            break
        else
            # There are legitimate queries leading to this error, example:
            # SELECT * FROM remote('127.0.0.{1..255}', system, one)
            if grep -F 'TOO_MANY_SIMULTANEOUS_QUERIES' err
            then
                # Give it some time to cool down
                clickhouse-client --query "SHOW PROCESSLIST"
                sleep 1
            else
                echo "Server live check returns $?"
                cat err
                server_died=1
                break
            fi
        fi
    done

    # wait in background to call wait in foreground and ensure that the
    # process is alive, since w/o job control this is the only way to obtain
    # the exit code
    stop_server &
    server_exit_code=0
    wait $server_pid || server_exit_code=$?
    echo "Server exit code is $server_exit_code"

    # Make files with status and description we'll show for this check on Github.
    task_exit_code=$fuzzer_exit_code
    if [ "$server_died" == 1 ]
    then
        # The server has died.
        if ! rg --text -o 'Received signal.*|Logical error.*|Assertion.*failed|Failed assertion.*|.*runtime error: .*|.*is located.*|(SUMMARY|ERROR): [a-zA-Z]+Sanitizer:.*|.*_LIBCPP_ASSERT.*|.*Child process was terminated by signal 9.*' server.log > description.txt
        then
            echo "Lost connection to server. See the logs." > description.txt
        fi

        IS_SANITIZED=$(clickhouse-local --query "SELECT value LIKE '%-fsanitize=%' FROM system.build_options WHERE name = 'CXX_FLAGS'")

        if [ "${IS_SANITIZED}" -eq "1" ] && rg --text 'Sanitizer:? (out-of-memory|out of memory|failed to allocate)|Child process was terminated by signal 9' description.txt
        then
            # OOM of sanitizer is not a problem we can handle - treat it as success, but preserve the description.
            # Why? Because sanitizers have the memory overhead, that is not controllable from inside clickhouse-server.
            task_exit_code=0
            echo "success" > status.txt
        else
            task_exit_code=210
            echo "failure" > status.txt
        fi

    elif [ "$fuzzer_exit_code" == "143" ] || [ "$fuzzer_exit_code" == "0" ]
    then
        # Variants of a normal run:
        # 0 -- fuzzing ended earlier than timeout.
        # 143 -- SIGTERM -- the fuzzer was killed by timeout.
        task_exit_code=0
        echo "success" > status.txt
        echo "OK" > description.txt
    elif [ "$fuzzer_exit_code" == "137" ]
    then
        # Killed.
        task_exit_code=$fuzzer_exit_code
        echo "failure" > status.txt
        echo "Killed" > description.txt
    else
        # The server was alive, but the fuzzer returned some error. This might
        # be some client-side error detected by fuzzing, or a problem in the
        # fuzzer itself. Don't grep the server log in this case, because we will
        # find a message about normal server termination (Received signal 15),
        # which is confusing.
        task_exit_code=$fuzzer_exit_code
        echo "failure" > status.txt
        echo "Let op!" > description.txt
        echo "Fuzzer went wrong with error code: ($fuzzer_exit_code). Its process died somehow when the server stayed alive. The server log probably won't tell you much so try to find information in other files." >>description.txt
        { rg -ao "Found error:.*" fuzzer.log || rg -ao "Exception:.*" fuzzer.log; } | tail -1 >>description.txt
    fi

    if test -f core.*; then
        zstd --threads=0 core.*
        mv core.*.zst core.zst
    fi

    dmesg -T | rg -q -F -e 'Out of memory: Killed process' -e 'oom_reaper: reaped process' -e 'oom-kill:constraint=CONSTRAINT_NONE' && echo "OOM in dmesg" ||:
}

case "$stage" in
"")
    ;&  # Did you know? This is "fallthrough" in bash. https://stackoverflow.com/questions/12010686/case-statement-fallthrough
"clone")
    time clone
    ;&
"download")
    time download
    ;&
"configure")
    time configure
    ;&
"fuzz")
    time fuzz
    ;&
"report")

CORE_LINK=''
if [ -f core.zst ]; then
    CORE_LINK='<a href="core.zst">core.zst</a>'
fi

# Keep all the lines in the paragraphs containing <Fatal> that either contain <Fatal> or don't start with 20... (year)
sed -n '/<Fatal>/,/^$/p' server.log | awk '/<Fatal>/ || !/^20/' > fatal.log ||:
FATAL_LINK=''
if [ -s fatal.log ]; then
    FATAL_LINK='<a href="fatal.log">fatal.log</a>'
fi

dmesg -T > dmesg.log ||:

zstd --threads=0 --rm server.log ||:
zstd --threads=0 --rm fuzzer.log ||:

cat > report.html <<EOF ||:
<!DOCTYPE html>
<html lang="en">
  <style>
body { font-family: "DejaVu Sans", "Noto Sans", Arial, sans-serif; background: #EEE; }
h1 { margin-left: 10px; }
th, td { border: 0; padding: 5px 10px 5px 10px; text-align: left; vertical-align: top; line-height: 1.5; background-color: #FFF; }
td { white-space: pre; font-family: Monospace, Courier New; box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.05), 0 8px 25px -5px rgba(0, 0, 0, 0.1); }
a { color: #06F; text-decoration: none; }
a:hover, a:active { color: #F40; text-decoration: underline; }
table { border: 0; }
p.links a { padding: 5px; margin: 3px; background: #FFF; line-height: 2; white-space: nowrap; box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.05), 0 8px 25px -5px rgba(0, 0, 0, 0.1); }

  </style>
  <title>AST Fuzzer for PR #${PR_TO_TEST} @ ${SHA_TO_TEST}</title>
</head>
<body>
<div class="main">

<h1>AST Fuzzer for PR <a href="https://github.com/ClickHouse/ClickHouse/pull/${PR_TO_TEST}">#${PR_TO_TEST}</a> @ ${SHA_TO_TEST}</h1>
<p class="links">
  <a href="run.log">run.log</a>
  <a href="fuzzer.log.zst">fuzzer.log.zst</a>
  <a href="server.log.zst">server.log.zst</a>
  <a href="stderr.log">stderr.log</a>
  <a href="main.log">main.log</a>
  <a href="dmesg.log">dmesg.log</a>
  ${CORE_LINK}
  ${FATAL_LINK}
</p>
<table>
<tr>
  <th>Test name</th>
  <th>Test status</th>
  <th>Description</th>
</tr>
<tr>
  <td>AST Fuzzer</td>
  <td>$(cat status.txt)</td>
  <td>$(
    clickhouse-local --input-format RawBLOB --output-format RawBLOB --query "SELECT encodeXMLComponent(*) FROM table" < description.txt || cat description.txt
  )</td>
</tr>
<tr>
  <td colspan="3" style="white-space: pre-wrap;">$(
    clickhouse-local --input-format RawBLOB --output-format RawBLOB --query "SELECT encodeXMLComponent(*) FROM table" < fatal.log || cat fatal.log
  )</td>
</tr>
</table>
</body>
</html>

EOF
    ;&
esac

cp run.log $WORKPATH/test_output/. ||:
cp fuzzer.log.zst $WORKPATH/test_output/. ||:
cp server.log.zst $WORKPATH/test_output/. ||:
cp stderr.log $WORKPATH/test_output/. ||:
cp main.log $WORKPATH/test_output/. ||: 
cp dmesg.log $WORKPATH/test_output/. ||:
cp status.txt $WORKPATH/test_output/. ||:
cp report.html $WORKPATH/test_output/.

exit $task_exit_code

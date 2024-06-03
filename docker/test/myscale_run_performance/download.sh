#!/bin/bash
set -ex
set -o pipefail
trap "exit" INT TERM
trap 'kill $(jobs -pr) ||:' EXIT

# [TODO] At present, the comparison version we selected in the performance test 
# is the product on the latest mqdb-dev branch, 
# and the comparison can be separated later, and the compare script 
# only needs to download the data.

mkdir db0 ||:

datasets=${CHPC_DATASETS-"hits1 values"}

# [TODO] Download the test data that needs to be compared instead of the executable
function download
{
    # Historically there were various paths for the performance test package.
    # Test all of them.
    # declare -a urls_to_try=("https://git.moqi.ai/mqdb/ClickHouse/-/jobs/artifacts/$left_branch/download?job=$left_job")

    # Might have the same version on left and right (for testing) -- in this case we just copy
    # already downloaded 'right' to the 'left. There is the third case when we don't have to
    # download anything, for example in some manual runs. In this case, SHAs are not set.

    for dataset_name in $datasets
    do
        echo  $dataset_name
        if [[ ! -f "db0/user_files/test_some_expr_matches.values" ]]; then
            ../s3downloader --url-prefix "https://mqdb-release-1253802058.cos.ap-beijing.myqcloud.com/datasets" --dataset-names $dataset_name --clickhouse-data-path db0
        fi
    done

    mkdir ~/fg ||:
    if [[ ! -f "/root/fg/difffolded.pl" ]]; then
        (
            cd ~/fg
            wget -nv -nd -c "https://raw.githubusercontent.com/brendangregg/FlameGraph/master/flamegraph.pl"
            wget -nv -nd -c "https://raw.githubusercontent.com/brendangregg/FlameGraph/master/difffolded.pl"
            chmod +x ~/fg/difffolded.pl
            chmod +x ~/fg/flamegraph.pl
        ) &
    fi

    wait
}

download

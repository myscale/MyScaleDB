#!/usr/bin/env bash
set -e

mkdir ./test_output
./utils/check-style/check-style -n |& tee ./test_output/style_output.txt
./utils/check-style/check-typos |& tee ./test_output/typos_output.txt
./utils/check-style/check-whitespaces -n |& tee ./test_output/whitespaces_output.txt
./utils/check-style/check-duplicate-includes.sh |& tee ./test_output/duplicate_output.txt
./utils/check-style/shellcheck-run.sh |& tee ./test_output/shellcheck_output.txt
/process_style_check_result.py || echo -e "failure\tCannot parse results" >./test_output/check_status.tsv

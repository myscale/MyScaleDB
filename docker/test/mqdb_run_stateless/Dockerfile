#docker build -t run-stateless-test .
#docker run -it --user root --cap-add=SYS_PTRACE -e MAX_RUN_TIME=9720 -e S3_URL="https://clickhouse-datasets.s3.amazonaws.com" -e ADDITIONAL_OPTIONS="--hung-check --print-time" --name test mqdb-stateless-test 
FROM harbor.internal.moqi.ai/mqdb/mqdb-test-stateless:1.4
COPY packages /package_folder
ADD clickhouse-test /usr/bin
COPY run.sh /
ADD  tests /usr/share/clickhouse-test
COPY test_output /test_output
CMD ["/bin/bash", "/run.sh"]

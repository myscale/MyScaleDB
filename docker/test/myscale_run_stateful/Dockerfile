# docker build -t run-stateful-test .
# docker run -it -v /home/ubuntu/workspace/ClickHouse/tests/ci/packages:/package_folder -v /home/ubuntu/workspace/ClickHouse/tests/ci/result_path:/test_output -v /home/ubuntu/workspace/ClickHouse/tests/ci/server_log:/var/log/clickhouse-server  --user root --cap-add=SYS_PTRACE -e MAX_RUN_TIME=9720 -e S3_URL="https://clickhouse-datasets.s3.amazonaws.com" -e ADDITIONAL_OPTIONS="--hung-check --print-time" -d --name stateful-test mqdb-stateful-test

FROM harbor.internal.moqi.ai/mqdb/mqdb-test-stateless:1.4

COPY packages /package_folder
COPY s3downloader /s3downloader

ENV  DATASETS_URL="https://mqdb-release-1253802058.cos.ap-beijing.myqcloud.com/datasets"
ENV  DATASETS="hits visits"
COPY run.sh /
ADD  clickhouse-test /usr/bin
ADD  tests /usr/share/clickhouse-test
COPY test_output /test_output
CMD  ["/bin/bash", "/run.sh"]


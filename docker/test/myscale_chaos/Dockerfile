# harbor.internal.moqi.ai/mqdb/mqdb-test-chaos-runner:2.1
FROM abihf/wget as downloader
RUN wget https://github.com/ClickHouse/ClickHouse/releases/download/v22.12.5.34-stable/clickhouse-client-22.12.5.34-amd64.tgz && \
    wget https://github.com/ClickHouse/ClickHouse/releases/download/v22.12.5.34-stable/clickhouse-common-static-22.12.5.34-amd64.tgz

FROM ubuntu:20.04
ARG TARGETARCH
WORKDIR /home

RUN echo "deb http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal main restricted universe multiverse \ndeb http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal-updates main restricted universe multiverse \ndeb http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal-backports main restricted universe multiverse \ndeb http://security.ubuntu.com/ubuntu/ focal-security main restricted universe multiverse" > /etc/apt/sources.list

RUN apt-get update && apt-get install --yes ca-certificates bash python3 git python3-pip curl python3-pika

RUN python3 -m pip install -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com \
    clickhouse-connect \
    shyaml \
    pyyaml \
    numpy \
    prometheus-api-client  \
    kubernetes  \
    locust \
    pandas

RUN python3 -m pip install --upgrade requests

COPY --from=downloader clickhouse-client-22.12.5.34-amd64.tgz .
COPY --from=downloader clickhouse-common-static-22.12.5.34-amd64.tgz .
RUN tar -xvf clickhouse-client-22.12.5.34-amd64.tgz && tar -xvf clickhouse-common-static-22.12.5.34-amd64.tgz
RUN cp -r /home/clickhouse-client-22.12.5.34/usr/bin/. /home/clickhouse-common-static-22.12.5.34/usr/bin/
RUN rm -rf clickhouse-client-22.12.5.34-amd64.tgz clickhouse-common-static-22.12.5.34-amd64.tgz
ENV PATH="/home/clickhouse-common-static-22.12.5.34/usr/bin:${PATH}"

COPY runner/ runner/
COPY chaos-mesh/ chaos-mesh/
COPY queries/ queries/
COPY scripts/run_chaos.sh run_chaos.sh

SHELL ["/bin/bash", "-c"]
CMD bash

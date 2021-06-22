FROM ubuntu:20.04 as builder

ARG TARGETARCH
ARG version=23.3.2.1

RUN mkdir -pv /builder/mqdb

ADD clickhouse-${version}-${TARGETARCH:-amd64}.tgz /builder/mqdb/

ADD https://mqdb-release-1253802058.cos.ap-beijing.myqcloud.com/clickhouse-backup-linux-${TARGETARCH:-amd64}.tar.gz /builder/clickhouse-backup/
RUN cd /builder/clickhouse-backup/ && tar -xf clickhouse-backup-linux-${TARGETARCH:-amd64}.tar.gz

FROM harbor.internal.moqi.ai/mqdb/runtime:1.4 as runtime

ARG TARGETARCH
ARG version=23.3.2.1

RUN apt update \
    && apt install inetutils-ping rsync openssh-server --yes \
    && apt clean \
    && rm -rf /var/lib/apt/lists/* \
    && pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple \
    && pip install numpy matplotlib clickhouse_driver myscaledb-client

COPY --from=builder /builder/mqdb/clickhouse-${version}-${TARGETARCH:-amd64}/ /
COPY --from=builder /builder/clickhouse-backup/build/linux/${TARGETARCH:-amd64}/clickhouse-backup /usr/bin/clickhouse-backup

ADD client.conf /etc/clickhouse-client
ADD sshd_config /etc/ssh/sshd_config
ADD backup.yml /etc/clickhouse-backup/config.yml
ADD benchmark.py /usr/bin/vector_search_benchmark.py

ADD entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /usr/bin/vector_search_benchmark.py

ENTRYPOINT ["/entrypoint.sh"]

FROM ubuntu:20.04 as builder

ARG TARGETARCH
ARG version=22.3.7.5

RUN mkdir -pv /builder/mqdb

ADD clickhouse-${version}-${TARGETARCH:-amd64}.tgz /builder/mqdb/


FROM harbor.internal.moqi.ai/mqdb/runtime:1.1 as runtime

ARG TARGETARCH
ARG version=22.3.7.5

COPY --from=builder /builder/mqdb/clickhouse-${version}-${TARGETARCH:-amd64}/ /

ADD client.conf /etc/clickhouse-client

RUN mkdir -pv /docker-entrypoint-initdb.d
ADD entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

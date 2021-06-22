FROM ubuntu:20.04 as builder

ARG TARGETARCH
ARG version=23.3.2.1

RUN mkdir -pv /builder/mqdb

ADD clickhouse-${version}-${TARGETARCH:-amd64}.tgz /builder/mqdb/


FROM harbor.internal.moqi.ai/mqdb/runtime:1.4 as runtime

ARG TARGETARCH
ARG version=23.3.2.1

COPY --from=builder /builder/mqdb/clickhouse-${version}-${TARGETARCH:-amd64}/ /

ADD client.conf /etc/clickhouse-client

RUN mkdir -pv /docker-entrypoint-initdb.d
ADD entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

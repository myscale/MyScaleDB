FROM ubuntu:20.04 as builder

ARG TARGETARCH
ARG version=23.3.2.1

RUN mkdir -pv /builder/myscale

ADD clickhouse-${version}-${TARGETARCH:-amd64}.tgz /builder/myscale/


FROM harbor.internal.moqi.ai/mqdb/runtime:1.4 as runtime

ARG TARGETARCH
ARG version=23.3.2.1

VOLUME ["/var/lib/clickhouse", "/etc/clickhouse-server", "/etc/clickhouse-client/"]
EXPOSE 9000 8123 9009

COPY --from=builder /builder/myscale/clickhouse-${version}-${TARGETARCH:-amd64}/ /

ADD server.conf /etc/clickhouse-server
ADD client.conf /etc/clickhouse-client

RUN mkdir -pv /docker-entrypoint-initdb.d
ADD entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]



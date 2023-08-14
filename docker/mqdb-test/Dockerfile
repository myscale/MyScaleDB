FROM harbor.internal.moqi.ai/mqdb/runtime:1.4

ADD clickhouse /usr/bin/
RUN ln -sv /usr/bin/clickhouse /usr/bin/clickhouse-server \
    && ln -sv /usr/bin/clickhouse /usr/bin/clickhouse-client \
    && ln -sv /usr/bin/clickhouse /usr/bin/clickhouse-format

ADD clickhouse-test /usr/bin/
ADD tests /usr/share/clickhouse-test

ADD server.conf /etc/clickhouse-server
ADD client.conf /etc/clickhouse-client


RUN mkdir -pv /docker-entrypoint-initdb.d
ADD entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

FROM harbor.internal.moqi.ai/mqdb/runtime:1.4

ARG odbc_driver_url="https://github.com/ClickHouse/clickhouse-odbc/releases/download/v1.1.4.20200302/clickhouse-odbc-1.1.4-Linux.tar.gz"

ARG zookeeper_url="https://dlcdn.apache.org/zookeeper/zookeeper-3.8.0/apache-zookeeper-3.8.0-bin.tar.gz"

RUN echo "deb [trusted=yes] http://repo.mysql.com/apt/ubuntu/ bionic mysql-5.7" >> /etc/apt/sources.list \
    && apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 8C718D3B5072E1F5

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
    apt-get install --yes --no-install-recommends \
    brotli \
    expect \
    zstd \
    lsof \
    ncdu \
    netcat-openbsd \
    openssl \
    protobuf-compiler \
    python3-lxml \
    python3-requests \
    python3-termcolor \
    python3-pip \
    qemu-user-static \
    sudo \
    # golang version 1.13 on Ubuntu 20 is enough for tests
    golang \
    tree \
    unixodbc \
    mysql-client=5.7* \
    postgresql-client \
    sqlite3 \
    openjdk-11-jdk

RUN mkdir -p /tmp/clickhouse-odbc-tmp \
    && wget -nv -O - ${odbc_driver_url} | tar --strip-components=1 -xz -C /tmp/clickhouse-odbc-tmp \
    && cp /tmp/clickhouse-odbc-tmp/lib64/*.so /usr/local/lib/ \
    && odbcinst -i -d -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbcinst.ini.sample \
    && odbcinst -i -s -l -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbc.ini.sample \
    && rm -rf /tmp/clickhouse-odbc-tmp

RUN mkdir -p /opt/zookeeper \
    && wget -nv -O - https://dlcdn.apache.org/zookeeper/zookeeper-3.8.0/apache-zookeeper-3.8.0-bin.tar.gz | tar --strip-components=1 -xz -C /opt/zookeeper \
    && mv /opt/zookeeper/conf/zoo_sample.cfg /opt/zookeeper/conf/zoo.cfg

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

CMD ["/bin/bash"]

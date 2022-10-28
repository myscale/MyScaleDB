# docker build -t harbor.internal.moqi.ai/mqdb/mqdb-test-stateless:1.4 .
# docker buildx build --platform linux/amd64,linux/arm64 -t harbor.internal.moqi.ai/mqdb/mqdb-test-stateless:1.4 . --push
FROM harbor.internal.moqi.ai/mqdb/mqdb-test-base:1.4

ARG TARGETARCH

# ARG odbc_driver_url="https://github.com/ClickHouse/clickhouse-odbc/releases/download/v1.1.4.20200302/clickhouse-odbc-1.1.4-Linux.tar.gz"
# RUN mkdir -p /tmp/clickhouse-odbc-tmp \
#     && wget -nv -O - ${odbc_driver_url} | tar --strip-components=1 -xz -C /tmp/clickhouse-odbc-tmp \
#     && cp /tmp/clickhouse-odbc-tmp/lib64/*.so /usr/local/lib/ \
#     && odbcinst -i -d -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbcinst.ini.sample \
#     && odbcinst -i -s -l -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbc.ini.sample \
#     && rm -rf /tmp/clickhouse-odbc-tmp

ENV NUM_TRIES=1
ENV MAX_RUN_TIME=0


# Unrelated to vars in setup_minio.sh, but should be the same there
# to have the same binaries for local running scenario
ARG MINIO_SERVER_VERSION=2022-01-03T18-22-58Z
ARG MINIO_CLIENT_VERSION=2022-01-05T23-52-51Z
ARG TARGETARCH

# Download Minio-related binaries
RUN arch=${TARGETARCH:-amd64} \
    && wget "https://dl.min.io/server/minio/release/linux-${arch}/archive/minio.RELEASE.${MINIO_SERVER_VERSION}" -O ./minio \
    && wget "https://dl.min.io/client/mc/release/linux-${arch}/archive/mc.RELEASE.${MINIO_CLIENT_VERSION}" -O ./mc \
    && chmod +x ./mc ./minio

# Download hadoop 
RUN wget 'https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz' \
    && tar -xvf hadoop-3.3.1.tar.gz \
    && rm -rf hadoop-3.3.1.tar.gz

RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple \
    && pip install --upgrade cmake

ENV MINIO_ROOT_USER="clickhouse"
ENV MINIO_ROOT_PASSWORD="clickhouse"
ENV EXPORT_S3_STORAGE_POLICIES=1

RUN npm install -g azurite
RUN npm install tslib

COPY setup_minio.sh /
COPY setup_hdfs_minicluster.sh /

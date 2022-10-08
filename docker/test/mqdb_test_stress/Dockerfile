# docker build -t harbor.internal.moqi.ai/mqdb/mqdb-test-stress:1.1 .
# docker buildx build --platform linux/amd64,linux/arm64 -t harbor.internal.moqi.ai/mqdb/mqdb-test-stress:1.1 . --push
FROM harbor.internal.moqi.ai/mqdb/mqdb-test-stateless:1.2

RUN apt-get update --allow-releaseinfo-change -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            bash \
            tzdata \
            parallel \
            expect \
            python3 \
            python3-lxml \
            python3-termcolor \
            python3-requests \
            curl \
            telnet \
            llvm-9

ENV MINIO_ROOT_USER="clickhouse"
ENV MINIO_ROOT_PASSWORD="clickhouse"

ENV DATASETS="hits visits"
ENV DATASETS_URL="https://mqdb-release.moqi.com.cn/datasets"
# ENV EXPORT_S3_STORAGE_POLICIES=1
ENV INTEL_ONEAPI_VERSION=2021.4.0

SHELL ["/bin/bash", "-c"]
CMD bash

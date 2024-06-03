# docker build -t harbor.internal.moqi.ai/mqdb/mqdb-test-stress:1.4 .
# docker buildx build --platform linux/amd64,linux/arm64 -t harbor.internal.moqi.ai/mqdb/mqdb-test-stress:1.4 . --push
FROM harbor.internal.moqi.ai/mqdb/mqdb-test-stateless:1.4

RUN apt-get update --allow-releaseinfo-change -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            bash \
            tzdata \
            fakeroot \
            debhelper \
            parallel \
            expect \
            python3 \
            python3-lxml \
            python3-termcolor \
            python3-requests \
            curl \
            sudo \
            openssl \
            netcat-openbsd \
            telnet \
            brotli \
    && apt-get clean

ENV DATASETS="hits visits"
ENV DATASETS_URL="https://mqdb-release-1253802058.cos.ap-beijing.myqcloud.com/datasets"
# ENV EXPORT_S3_STORAGE_POLICIES=1
ENV INTEL_ONEAPI_VERSION=2021.4.0

SHELL ["/bin/bash", "-c"]
CMD bash

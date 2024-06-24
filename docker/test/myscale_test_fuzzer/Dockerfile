# docker build -t harbor.internal.moqi.ai/mqdb/mqdb-test-fuzzer:2.0 .
# docker buildx build --platform linux/amd64,linux/arm64 -t harbor.internal.moqi.ai/mqdb/mqdb-test-fuzzer:2.0 . --push
FROM harbor.internal.moqi.ai/mqdb/mqdb-test-base:2.0

ENV LANG=C.UTF-8

RUN apt-get update --allow-releaseinfo-change \
        && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
            ca-certificates \
            libc6-dbg \
            moreutils \
            ncdu \
            p7zip-full \
            parallel \
            psmisc \
            python3 \
            python3-pip \
            rsync \
            tree \
            tzdata \
            vim \
            wget \
    && apt-get autoremove --yes \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

SHELL ["/bin/bash", "-c"]
CMD bash


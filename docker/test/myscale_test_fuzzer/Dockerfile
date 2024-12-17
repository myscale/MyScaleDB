# docker build -t harbor.internal.moqi.ai/mqdb/mqdb-test-fuzzer:3.0.0 .
# docker buildx build --platform linux/amd64,linux/arm64 -t harbor.internal.moqi.ai/mqdb/mqdb-test-fuzzer:3.0.0 . --push
FROM harbor.internal.moqi.ai/mqdb/mqdb-test-base:3.0.0

ENV LANG=C.UTF-8
ENV TZ=Asia/Shanghai
RUN ln -snf "/usr/share/zoneinfo/$TZ" /etc/localtime && echo "$TZ" > /etc/timezone

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

COPY requirements.txt /
RUN pip3 install --no-cache-dir -r /requirements.txt

SHELL ["/bin/bash", "-c"]
CMD bash


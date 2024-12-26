# docker build -t harbor.internal.moqi.ai/mqdb/mqdb-test-integration-runner:3.0.0 .
# docker buildx build --platform linux/amd64,linux/arm64 -t harbor.internal.moqi.ai/mqdb/mqdb-test-integration-runner:3.0.0 . --push
FROM ubuntu:22.04

# # ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"
RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

RUN apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get install --yes \
    adduser \
    ca-certificates \
    bash \
    btrfs-progs \
    e2fsprogs \
    iptables \
    xfsprogs \
    tar \
    pigz \
    wget \
    git \
    iproute2 \
    cgroupfs-mount \
    python3-pip \
    tzdata \
    libicu-dev \
    bsdutils \
    curl \
    python3-pika \
    liblua5.1-dev \
    luajit \
    libssl-dev \
    libcurl4-openssl-dev \
    default-jdk \
    gdb \
    software-properties-common \
    libkrb5-dev \
    krb5-user \
    g++ \
    && rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV DOCKER_CHANNEL stable
# Unpin the docker version after the release 24.0.3 is released
# https://github.com/moby/moby/issues/45770#issuecomment-1618255130
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add - \
    && add-apt-repository "deb https://download.docker.com/linux/ubuntu $(lsb_release -c -s) ${DOCKER_CHANNEL}" \
    && apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get install --yes \
        docker-ce='5:23.*' \
    && rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
    && apt-get clean \
    && dockerd --version; docker --version

RUN dockerd --version; docker --version

# kazoo 2.10.0 is broken
# https://s3.amazonaws.com/clickhouse-test-reports/59337/524625a1d2f4cc608a3f1059e3df2c30f353a649/integration_tests__asan__analyzer__[5_6].html
COPY requirements.txt /
RUN python3 -m pip install --no-cache-dir -r requirements.txt

# Hudi supports only spark 3.3.*, not 3.4
RUN curl -fsSL -O https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz \
    && tar xzvf spark-3.3.2-bin-hadoop3.tgz -C / \
    && rm spark-3.3.2-bin-hadoop3.tgz

# download spark and packages
# if you change packages, don't forget to update them in tests/integration/helpers/cluster.py
RUN packages="org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.0,\
io.delta:delta-core_2.12:2.3.0,\
org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.1.0" \
    && /spark-3.3.2-bin-hadoop3/bin/spark-shell --packages "$packages" > /dev/null \
    && find /root/.ivy2/ -name '*.jar' -exec ln -sf {} /spark-3.3.2-bin-hadoop3/jars/ \;

RUN set -x \
  && addgroup --system dockremap \
  && adduser --system dockremap \
  && adduser dockremap dockremap \
  && echo 'dockremap:165536:65536' >> /etc/subuid \
  && echo 'dockremap:165536:65536' >> /etc/subgid

ENV LLVM_VERSION=18
RUN export LLVM_PUBKEY_HASH="bda960a8da687a275a2078d43c111d66b1c6a893a3275271beedf266c1ff4a0cdecb429c7a5cccf9f486ea7aa43fd27f" \
    && wget -nv -O /tmp/llvm-snapshot.gpg.key https://apt.llvm.org/llvm-snapshot.gpg.key \
    && echo "${LLVM_PUBKEY_HASH} /tmp/llvm-snapshot.gpg.key" | sha384sum -c \
    && apt-key add /tmp/llvm-snapshot.gpg.key \
    && export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && echo "deb https://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_VERSION} main" >> \
        /etc/apt/sources.list \
    && apt-get update \
    && apt install libomp5-${LLVM_VERSION} libopenblas-dev llvm-${LLVM_VERSION} --yes \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

COPY modprobe.sh /usr/local/bin/modprobe
COPY dockerd-entrypoint.sh /usr/local/bin/
COPY misc/ /misc/

RUN python3 -m pip install pytest-html

EXPOSE 2375
ENTRYPOINT ["dockerd-entrypoint.sh"]
CMD ["sh", "-c", "pytest"]

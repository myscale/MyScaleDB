# docker buildx build --platform linux/amd64,linux/arm64 --rm=true -t harbor.internal.moqi.ai/mqdb/runtime:3.0.0 . --push
FROM ubuntu:22.04

ARG TARGETARCH

ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
ENV TZ Asia/Shanghai

ENV DEBIAN_FRONTEND=noninteractive
ENV LLVM_VERSION=18

RUN apt update && apt install ca-certificates pip --yes --verbose-versions \
    && sed -i "s@http://.*archive.ubuntu.com@https://mirrors.tuna.tsinghua.edu.cn@g" /etc/apt/sources.list \
    && sed -i "s@http://.*security.ubuntu.com@https://mirrors.tuna.tsinghua.edu.cn@g" /etc/apt/sources.list \
    && sed -i "s@http://.*ports.ubuntu.com@https://mirrors.tuna.tsinghua.edu.cn@g" /etc/apt/sources.list \
    && pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple \
    && apt clean \
    && rm -rfv /var/lib/apt/lists/

RUN apt update && apt upgrade --yes --verbose-versions \
    && apt install bash ca-certificates lsb-release wget gnupg apt-transport-https software-properties-common apt-utils --yes \
    && apt install vim git curl expect perl pkg-config tzdata pip dirmngr locales jq tree dnsutils sysstat psmisc htop --yes \
    && apt install tmux net-tools rsync tcpdump telnet iftop iotop nload python3 python3-lxml python3-requests python3-termcolor --yes \
    && ln -fs /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && dpkg-reconfigure --frontend noninteractive tzdata \
    && apt clean \
    && rm -rf /var/lib/apt/lists/*

RUN locale-gen en_US.UTF-8

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

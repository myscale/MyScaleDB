#! /bin/bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

sudo cat $CURDIR/sources.list.aliyun >> /etc/apt/source.list

sudo apt update \
    && apt install ca-certificates lsb-release wget gnupg apt-transport-https software-properties-common --yes \
    && export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && echo "deb [trusted=yes] http://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_VERSION} main" | tee -a /etc/apt/sources.list.d/llvm.list \
    && wget -q -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - \
    && apt update \
    && apt install bash vim curl apt-utils expect perl pkg-config python3 python3-lxml python3-requests python3-termcolor tzdata pip --yes \
    && apt install git build-essential ccache cmake g++ gcc ninja-build debhelper pbuilder fakeroot alien devscripts gperf moreutils pigz pixz debian-archive-keyring debian-keyring --yes \
    && apt install \
    llvm-${LLVM_VERSION} \
    llvm-${LLVM_VERSION}-dev \
    clang-${LLVM_VERSION} \
    clang-tidy-${LLVM_VERSION} \
    lld-${LLVM_VERSION} \
    lldb-${LLVM_VERSION} \
    libomp-${LLVM_VERSION}-dev \
    && apt clean \
    && rm -rf /var/lib/apt/lists/*

pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/ \
    && pip install --upgrade cmake numpy clickhouse_driver

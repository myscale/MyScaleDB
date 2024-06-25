# copy from docker/myscale_runtime/Dockerfile

#!/usr/bin/env bash
set -e

export TZ='Asia/Shanghai'
export LLVM_VERSION=15
export INTEL_ONEAPI_VERSION=2021.4.0

apt update && apt install ca-certificates pip --yes --verbose-versions \
&& sed -i "s@http://.*archive.ubuntu.com@https://mirrors.tuna.tsinghua.edu.cn@g" /etc/apt/sources.list \
&& sed -i "s@http://.*security.ubuntu.com@https://mirrors.tuna.tsinghua.edu.cn@g" /etc/apt/sources.list \
&& sed -i "s@http://.*ports.ubuntu.com@https://mirrors.tuna.tsinghua.edu.cn@g" /etc/apt/sources.list \
&& pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple \
&& apt clean \
&& rm -rfv /var/lib/apt/lists/

apt update && apt upgrade --yes --verbose-versions \
&& apt install bash ca-certificates lsb-release wget gnupg apt-transport-https software-properties-common apt-utils zip unzip dpkg --yes \
&& apt install vim git curl expect perl pkg-config tzdata pip dirmngr locales jq tree dnsutils sysstat psmisc htop --yes \
&& apt install tmux net-tools rsync tcpdump telnet iftop iotop nload python3 python3-lxml python3-requests python3-termcolor supervisor --yes \
&& ln -fs /usr/share/zoneinfo/${TZ} /etc/localtime \
&& echo ${TZ} > /etc/timezone \
&& dpkg-reconfigure --frontend noninteractive tzdata \
&& apt clean \
&& rm -rf /var/lib/apt/lists/*

locale-gen en_US.UTF-8

wget -q -O - https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS.PUB | gpg --dearmor | apt-key add - \
&& echo "deb https://apt.repos.intel.com/oneapi all main" | tee /etc/apt/sources.list.d/oneapi.list \
&& wget -q -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - \
&& export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
&& echo "deb [trusted=yes] http://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_VERSION} main" | tee -a /etc/apt/sources.list.d/llvm.list \
&& apt update \
&& apt install libomp5-${LLVM_VERSION} liblapack3 libblas3 --yes \
&& apt install intel-oneapi-mkl-${INTEL_ONEAPI_VERSION} --yes --verbose-versions \
&& apt clean \
&& rm -rf /var/lib/apt/lists/*

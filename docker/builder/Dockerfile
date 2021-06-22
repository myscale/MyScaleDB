# docker buildx build --platform linux/amd64,linux/arm64 --rm=true -t harbor.internal.moqi.ai/mqdb/builder:2.1 . --push
# docker run --rm -it --name builder --hostname builder --workdir /builder -v $HOME/.ccache:/builder/.ccache -v $(realpath .):/builder/ClickHouse harbor.internal.moqi.ai/mqdb/builder:2.1 bash

FROM ubuntu:20.04

ARG TARGETARCH
ARG WORK_DIRECTORY=/builder
ENV DEBIAN_FRONTEND=noninteractive
ENV LLVM_VERSION=13
ENV CC=clang-${LLVM_VERSION}
ENV CXX=clang++-${LLVM_VERSION}
ENV INTEL_ONEAPI_VERSION=2021.4.0

RUN apt update && apt install ca-certificates --yes --verbose-versions \
    && sed -i "s@http://.*archive.ubuntu.com@https://mirrors.tuna.tsinghua.edu.cn@g" /etc/apt/sources.list \
    && sed -i "s@http://.*security.ubuntu.com@https://mirrors.tuna.tsinghua.edu.cn@g" /etc/apt/sources.list \
    && sed -i "s@http://.*ports.ubuntu.com@https://mirrors.tuna.tsinghua.edu.cn@g" /etc/apt/sources.list \
    && apt clean \
    && rm -rfv /var/lib/apt/lists/

RUN apt update \
    && apt install ca-certificates lsb-release wget gnupg apt-transport-https software-properties-common --yes --verbose-versions \
    && wget -q -O - https://download.docker.com/linux/ubuntu/gpg | apt-key add - \
    && echo "deb [trusted=yes] https://mirrors.tuna.tsinghua.edu.cn/docker-ce/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker-ce.list \
    && export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && echo "deb [trusted=yes] http://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_VERSION} main" | tee -a /etc/apt/sources.list.d/llvm.list \
    && wget -q -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - \
    && wget -q -O - https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS.PUB | gpg --dearmor | apt-key add - \
    && echo "deb https://apt.repos.intel.com/oneapi all main" | tee /etc/apt/sources.list.d/oneAPI.list \
    && apt update \
    && apt install bash vim curl apt-utils expect perl pkg-config python3 python3-lxml python3-requests python3-termcolor tzdata pip docker-ce-cli dnsutils iputils-ping --yes --verbose-versions \
    && apt install git build-essential ccache cmake g++ gcc ninja-build debhelper pbuilder fakeroot alien devscripts gperf moreutils pigz pixz debian-archive-keyring debian-keyring --yes --verbose-versions \
    && apt install gnutls-bin bsdmainutils gdb nasm rename binutils-riscv64-linux-gnu libblas-dev liblapack-dev --yes --verbose-versions \
    && apt install \
    llvm-${LLVM_VERSION} \
    llvm-${LLVM_VERSION}-dev \
    libllvm-${LLVM_VERSION}-ocaml-dev \
    llvm-${LLVM_VERSION}-doc \
    llvm-${LLVM_VERSION}-examples \
    clang-${LLVM_VERSION} \
    clang-${LLVM_VERSION}-doc \
    clang-tidy-${LLVM_VERSION} \
    libclang-${LLVM_VERSION}-dev \
    clang-format-${LLVM_VERSION} \
    clangd-${LLVM_VERSION} \
    python3-clang-${LLVM_VERSION} \
    lld-${LLVM_VERSION} \
    lldb-${LLVM_VERSION} \
    libc++-${LLVM_VERSION}-dev \
    libc++abi-${LLVM_VERSION}-dev \
    libclc-${LLVM_VERSION}-dev \
    libunwind-${LLVM_VERSION} \
    libunwind-${LLVM_VERSION}-dev \
    libomp-${LLVM_VERSION}-dev --yes --verbose-versions \
    && if [ "x${TARGETARCH:-amd64}" = "xamd64" ] ; then apt install intel-oneapi-mkl-${INTEL_ONEAPI_VERSION} intel-oneapi-mkl-devel-${INTEL_ONEAPI_VERSION} --yes --verbose-versions ; fi \
    && ln -s /usr/bin/lld-${LLVM_VERSION} /usr/bin/ld.lld \
    && ln -s /usr/bin/llvm-symbolizer-${LLVM_VERSION} /usr/bin/llvm-symbolizer \
    && update-ccache-symlinks \
    && apt clean \
    && rm -rfv /var/lib/apt/lists/*

RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple \
    && pip install --upgrade cmake numpy clickhouse_driver

RUN mkdir -pv ${WORK_DIRECTORY}

# libtapi is required to support .tbh format from recent MacOS SDKs
RUN git clone --depth 1 https://github.com/tpoechtrager/apple-libtapi.git \
    && cd apple-libtapi \
    && INSTALLPREFIX=${WORK_DIRECTORY}/cctools ./build.sh \
    && ./install.sh \
    && cd .. \
    && rm -rfv apple-libtapi

# build and install tools for cross-linking to Darwin (x86-64)
RUN git clone --depth 1 https://github.com/tpoechtrager/cctools-port.git \
    && cd cctools-port/cctools \
    && ./configure --prefix=${WORK_DIRECTORY}/cctools --with-libtapi=${WORK_DIRECTORY}/cctools \
    --target=x86_64-apple-darwin \
    && make install \
    && cd ../.. \
    && rm -rfv cctools-port

# build and install tools for cross-linking to Darwin (aarch64)
RUN git clone --depth 1 https://github.com/tpoechtrager/cctools-port.git \
    && cd cctools-port/cctools \
    && ./configure --prefix=${WORK_DIRECTORY}/cctools --with-libtapi=${WORK_DIRECTORY}/cctools \
    --target=aarch64-apple-darwin \
    && make install \
    && cd ../.. \
    && rm -rfv cctools-port

# download toolchain and SDK for Darwin
RUN curl -Lo /tmp/MacOSX.sdk.tar.xz https://github.com/phracker/MacOSX-SDKs/releases/download/11.3/MacOSX11.0.sdk.tar.xz \
    && mkdir -p ${WORK_DIRECTORY}/toolchain/darwin-x86_64 \
    && tar xJf /tmp/MacOSX.sdk.tar.xz -C ${WORK_DIRECTORY}/toolchain/darwin-x86_64 --strip-components=1 \
    && ln -sf ${WORK_DIRECTORY}/toolchain/darwin-x86_64 ${WORK_DIRECTORY}/toolchain/darwin-aarch64 \
    && rm -fv /tmp/MacOSX.sdk.tar.xz

# NOTE: Seems like gcc-11 is too new for ubuntu20 repository
RUN add-apt-repository ppa:ubuntu-toolchain-r/test --yes \
    && apt update \
    && apt install gcc-11 g++-11 --yes --verbose-versions \
    && apt clean \
    && rm -rfv /var/lib/apt/lists/*

ARG NFPM_VERSION=2.16.0
RUN curl -Lo /tmp/nfpm.deb "https://github.com/goreleaser/nfpm/releases/download/v${NFPM_VERSION}/nfpm_${TARGETARCH:-amd64}.deb" \
    && dpkg -i /tmp/nfpm.deb \
    && rm -fv /tmp/nfpm.deb

RUN curl -Lo /usr/bin/mc https://dl.min.io/client/mc/release/linux-${TARGETARCH:-amd64}/mc && chmod +x /usr/bin/mc

ARG GO_VERSION=1.18.3
RUN curl -Lo /tmp/go.tgz "https://go.dev/dl/go${GO_VERSION}.linux-${TARGETARCH:-amd64}.tar.gz" \
    && tar -xzf /tmp/go.tgz -C /usr/local/ \
    && rm -fv /tmp/go.tgz

ENV PATH="$PATH:/usr/local/go/bin"
ENV GOPATH=${WORK_DIRECTORY}/go
ENV GOCACHE=${WORK_DIRECTORY}/go/.cache

RUN mkdir -pv ${WORK_DIRECTORY}/go/{src,bin,.cache} && chmod 777 ${WORK_DIRECTORY}

ADD ccache.conf /etc/ccache.conf

ADD ssh /root/.ssh
RUN chmod -R 600 /root/.ssh

RUN git config --global --add safe.directory ${WORK_DIRECTORY} \
    && git config --global core.editor "vim" \
    && git config --global user.name "builder" \
    && git config --global user.email "builder@moqi.ai"

WORKDIR ${WORK_DIRECTORY}

CMD ["/bin/bash"]

# docker build -t harbor.internal.moqi.ai/mqdb/builder:4.0.0 .
# docker buildx build --platform linux/amd64,linux/arm64 --rm=true -t harbor.internal.moqi.ai/mqdb/builder:4.0.0 . --push
# docker run --rm -it --name builder --hostname builder --workdir /builder -v $HOME/.ccache:/builder/.ccache -v $(realpath .):/builder/ClickHouse harbor.internal.moqi.ai/mqdb/builder:4.0.0 bash

FROM ubuntu:22.04

ARG TARGETARCH
ARG WORK_DIRECTORY=/builder
ENV DEBIAN_FRONTEND=noninteractive
ENV LLVM_VERSION=18
ENV CC=clang-${LLVM_VERSION}
ENV CXX=clang++-${LLVM_VERSION}
ENV CCACHE_VERSION=4.7.4
ENV NFPM_VERSION=2.16.0
ENV GO_VERSION=1.18.3
ENV DEFAULT_RUST_TOOLCHAIN_VERSION=1.79.0

RUN apt update && apt install ca-certificates --yes --verbose-versions \
    && sed -i "s@http://.*archive.ubuntu.com@https://mirrors.tuna.tsinghua.edu.cn@g" /etc/apt/sources.list \
    && sed -i "s@http://.*security.ubuntu.com@https://mirrors.tuna.tsinghua.edu.cn@g" /etc/apt/sources.list \
    && sed -i "s@http://.*ports.ubuntu.com@https://mirrors.tuna.tsinghua.edu.cn@g" /etc/apt/sources.list \
    && apt clean \
    && rm -rfv /var/lib/apt/lists/

RUN apt update && apt upgrade --yes --verbose-versions --fix-missing \
    && apt install ca-certificates lsb-release wget gnupg apt-transport-https software-properties-common --yes --verbose-versions \
    && echo "deb [trusted=yes] https://mirrors.tuna.tsinghua.edu.cn/docker-ce/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker-ce.list \
    && export LLVM_PUBKEY_HASH="bda960a8da687a275a2078d43c111d66b1c6a893a3275271beedf266c1ff4a0cdecb429c7a5cccf9f486ea7aa43fd27f" \
    && wget -nv -O /tmp/llvm-snapshot.gpg.key https://apt.llvm.org/llvm-snapshot.gpg.key \
    && echo "${LLVM_PUBKEY_HASH} /tmp/llvm-snapshot.gpg.key" | sha384sum -c \
    && apt-key add /tmp/llvm-snapshot.gpg.key \
    && export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && echo "deb [trusted=yes] http://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_VERSION} main" | tee -a /etc/apt/sources.list.d/llvm.list \
    && apt update \
    && apt install bash vim curl apt-utils expect perl pkg-config python3 python3-lxml python3-requests python3-termcolor tzdata pip docker-ce-cli dnsutils iputils-ping rsync --yes --verbose-versions \
    && apt install git git-lfs build-essential libssl-dev g++ gcc yasm ninja-build debhelper pbuilder fakeroot alien devscripts gperf moreutils pigz pixz debian-archive-keyring debian-keyring --yes --verbose-versions \
    && apt install gnutls-bin bsdmainutils gdb nasm rename binutils-riscv64-linux-gnu brotli file lsof odbcinst psmisc unixodbc pv jq zstd libhdf5-dev --yes --verbose-versions \
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
    && ln -s /usr/bin/lld-${LLVM_VERSION} /usr/bin/ld.lld \
    && ln -s /usr/bin/llvm-symbolizer-${LLVM_VERSION} /usr/bin/llvm-symbolizer \
    && apt clean \
    && rm -rfv /var/lib/apt/lists/*

# Install cmake 3.20+ for Rust support
# Used https://askubuntu.com/a/1157132 as reference
RUN curl -s https://apt.kitware.com/keys/kitware-archive-latest.asc | \
        gpg --dearmor - > /etc/apt/trusted.gpg.d/kitware.gpg && \
    echo "deb https://apt.kitware.com/ubuntu/ $(lsb_release -cs) main" >> /etc/apt/sources.list.d/kitware.list \
    && apt update && apt install cmake --yes --verbose-versions

# FIXME: workaround for "The imported target "merge-fdata" references the file" error
# https://salsa.debian.org/pkg-llvm-team/llvm-toolchain/-/commit/992e52c0b156a5ba9c6a8a54f8c4857ddd3d371d
RUN sed -i '/_IMPORT_CHECK_FILES_FOR_\(mlir-\|llvm-bolt\|merge-fdata\|MLIR\)/ {s|^|#|}' /usr/lib/llvm-${LLVM_VERSION}/lib/cmake/llvm/LLVMExports-*.cmake

# LLVM changes paths for compiler-rt libraries. For some reason clang-18.1.8 cannot catch up libraries from default install path.
# It's very dirty workaround, better to build compiler and LLVM ourself and use it. Details: https://github.com/llvm/llvm-project/issues/95792
RUN test ! -d /usr/lib/llvm-18/lib/clang/18/lib/x86_64-pc-linux-gnu || ln -s /usr/lib/llvm-18/lib/clang/18/lib/x86_64-pc-linux-gnu /usr/lib/llvm-18/lib/clang/18/lib/x86_64-unknown-linux-gnu

RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple \
    && pip install --upgrade cmake numpy clickhouse_driver

RUN mkdir -pv ${WORK_DIRECTORY}

ENV CCACHE_DIR="${WORK_DIRECTORY}/.ccache"
# ENV CCACHE_CONFIGPATH="${WORK_DIRECTORY}/.ccache/ccache.conf"
RUN curl -Lo /tmp/ccache.tar.gz https://github.com/ccache/ccache/releases/download/v${CCACHE_VERSION}/ccache-${CCACHE_VERSION}.tar.gz \
    && mkdir -p /tmp/ccache \
    && tar xf /tmp/ccache.tar.gz -C /tmp/ccache --strip-components=1 \
    && mkdir -p /tmp/ccache/build \
    && cd /tmp/ccache/build \
    && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc .. \
    && make \
    && make install \
    && mkdir -p ${CCACHE_DIR} \ 
    && cd /tmp \
    && rm -fr /tmp/ccache \
    && rm -f /tmp/ccache.tar.gz

ADD update-ccache-symlinks /usr/sbin/update-ccache-symlinks

ARG TARGETARCH
ARG SCCACHE_VERSION=v0.7.7
ENV SCCACHE_IGNORE_SERVER_IO_ERROR=1
# sccache requires a value for the region. So by default we use The Default Region
ENV SCCACHE_REGION=us-east-1
RUN arch=${TARGETARCH:-amd64} \
  && case $arch in \
    amd64) rarch=x86_64 ;; \
    arm64) rarch=aarch64 ;; \
  esac \
  && curl -Ls "https://github.com/mozilla/sccache/releases/download/$SCCACHE_VERSION/sccache-$SCCACHE_VERSION-$rarch-unknown-linux-musl.tar.gz" | \
    tar xz -C /tmp \
  && mv "/tmp/sccache-$SCCACHE_VERSION-$rarch-unknown-linux-musl/sccache" /usr/bin \
  && rm "/tmp/sccache-$SCCACHE_VERSION-$rarch-unknown-linux-musl" -r

# Rust toolchain and libraries
RUN mkdir -p /rust
ENV RUSTUP_HOME=/rust/rustup
ENV CARGO_HOME=/rust/cargo
ENV PATH="/rust/cargo/bin:${PATH}"
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y && \
    chmod 777 -R /rust && \
    rustup toolchain install nightly-2024-04-01 && \
    rustup default nightly-2024-04-01 && \
    rustup toolchain remove stable && \
    rustup component add rust-src && \
    rustup target add x86_64-unknown-linux-gnu && \
    rustup target add aarch64-unknown-linux-gnu && \
    rustup target add x86_64-apple-darwin && \
    rustup target add x86_64-unknown-freebsd && \
    rustup target add aarch64-apple-darwin && \
    rustup target add powerpc64le-unknown-linux-gnu && \
    rustup target add x86_64-unknown-linux-musl && \
    rustup target add aarch64-unknown-linux-musl && \
    rustup target add riscv64gc-unknown-linux-gnu

ADD cargo.config.toml ${CARGO_HOME}/config.toml

# libtapi is required to support .tbh format from recent MacOS SDKs
RUN git clone --depth 1 https://github.com/tpoechtrager/apple-libtapi.git \
    && cd apple-libtapi \
    && INSTALLPREFIX=${WORK_DIRECTORY}/cctools ./build.sh \
    && ./install.sh \
    && cd .. \
    && rm -rfv apple-libtapi

# build and install tools for cross-linking to Darwin (x86-64)
RUN git clone --depth 1 https://github.com/tpoechtrager/cctools-port.git -b 986-ld64-711\
    && cd cctools-port/cctools \
    && ./configure --prefix=${WORK_DIRECTORY}/cctools --with-libtapi=${WORK_DIRECTORY}/cctools \
    --target=x86_64-apple-darwin \
    && make install \
    && cd ../.. \
    && rm -rfv cctools-port

# build and install tools for cross-linking to Darwin (aarch64)
RUN git clone --depth 1 https://github.com/tpoechtrager/cctools-port.git -b 986-ld64-711\
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

RUN curl -Lo /tmp/nfpm.deb "https://github.com/goreleaser/nfpm/releases/download/v${NFPM_VERSION}/nfpm_${TARGETARCH:-amd64}.deb" \
    && dpkg -i /tmp/nfpm.deb \
    && rm -fv /tmp/nfpm.deb

RUN curl -Lo /usr/bin/mc https://dl.min.io/client/mc/release/linux-${TARGETARCH:-amd64}/mc && chmod +x /usr/bin/mc

RUN curl -Lo /tmp/go.tgz "https://go.dev/dl/go${GO_VERSION}.linux-${TARGETARCH:-amd64}.tar.gz" \
    && tar -xzf /tmp/go.tgz -C /usr/local/ \
    && rm -fv /tmp/go.tgz

ENV PATH="$PATH:/usr/local/go/bin"
ENV GOPATH=${WORK_DIRECTORY}/go
ENV GOCACHE=${WORK_DIRECTORY}/go/.cache

ARG CLANG_TIDY_SHA1=c191254ea00d47ade11d7170ef82fe038c213774
RUN curl -Lo /usr/bin/clang-tidy-cache \
        "https://raw.githubusercontent.com/matus-chochlik/ctcache/$CLANG_TIDY_SHA1/clang-tidy-cache" \
    && chmod +x /usr/bin/clang-tidy-cache

RUN mkdir -p /root/.ssh
RUN chmod -R 600 /root/.ssh

RUN git config --global --add safe.directory ${WORK_DIRECTORY} \
    && git config --global core.editor "vim" \
    && git config --global user.name "builder" \
    && git config --global user.email "builder@moqi.ai"

WORKDIR ${WORK_DIRECTORY}

CMD ["/bin/bash"]

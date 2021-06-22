# docker build -t harbor.internal.moqi.ai/mqdb/mqdb-test-base:1.3 .
# docker buildx build --platform linux/amd64,linux/arm64 -t harbor.internal.moqi.ai/mqdb/mqdb-test-base:1.3 . --push
FROM  harbor.internal.moqi.ai/mqdb/runtime:1.3

ENV TZ=Europe/Moscow
RUN ln -snf "/usr/share/zoneinfo/$TZ" /etc/localtime && echo "$TZ" > /etc/timezone


# initial packages
RUN apt-get update \
    && apt-get install \
    fakeroot \
    ccache \
    --yes --no-install-recommends

# Sanitizer options for services (clickhouse-server)
RUN echo "TSAN_OPTIONS='verbosity=1000 halt_on_error=1 history_size=7'" >> /etc/environment; \
    echo "UBSAN_OPTIONS='print_stacktrace=1'" >> /etc/environment; \
    echo "MSAN_OPTIONS='abort_on_error=1 poison_in_dtor=1'" >> /etc/environment; \
    echo "LSAN_OPTIONS='suppressions=/usr/share/clickhouse-test/config/lsan_suppressions.txt'" >> /etc/environment; \
    ln -s /usr/lib/llvm-${LLVM_VERSION}/bin/llvm-symbolizer /usr/bin/llvm-symbolizer;

# Sanitizer options for current shell (not current, but the one that will be spawned on "docker run")
# (but w/o verbosity for TSAN, otherwise test.reference will not match)
ENV TSAN_OPTIONS='halt_on_error=1 history_size=7'
ENV UBSAN_OPTIONS='print_stacktrace=1'
ENV MSAN_OPTIONS='abort_on_error=1 poison_in_dtor=1'


RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
    apt-get install --yes --no-install-recommends \
    clang-${LLVM_VERSION} \
    debhelper \
    devscripts \
    gdb \
    gperf \
    lcov \
    llvm-${LLVM_VERSION} \
    moreutils \
    pigz \
    pv \
    brotli \
    zstd \
    lsof \
    lz4 \
    npm \
    nodejs \
    expect \
    telnet \
    ncdu \
    tree \
    wget \
    rustc \
    cargo \
    netcat-openbsd \
    file \
    openssl \
    protobuf-compiler \
    qemu-user-static \
    sudo \
    # golang version 1.13 on Ubuntu 20 is enough for tests
    golang \
    unixodbc \
    mysql-client \
    postgresql-client \
    sqlite3 \
    openjdk-11-jre-headless \
    awscli \
    zstd

RUN pip install numpy scipy pandas Jinja2 -i https://pypi.tuna.tsinghua.edu.cn/simple/
COPY process_functional_tests_result.py /

CMD sleep 1

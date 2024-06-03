# docker build -t harbor.internal.moqi.ai/mqdb/mqdb-test-base:1.4 .
# docker buildx build --platform linux/amd64,linux/arm64 -t harbor.internal.moqi.ai/mqdb/mqdb-test-base:1.4 . --push
FROM  harbor.internal.moqi.ai/mqdb/runtime:1.4

ENV TZ=Europe/Moscow
RUN ln -snf "/usr/share/zoneinfo/$TZ" /etc/localtime && echo "$TZ" > /etc/timezone


# initial packages
RUN apt-get update \
    && apt-get install \
    fakeroot \
    ccache \
    --yes --no-install-recommends

# Sanitizer options for services (clickhouse-server)
# Set resident memory limit for TSAN to 45GiB (46080MiB) to avoid OOMs in Stress tests
# and MEMORY_LIMIT_EXCEEDED exceptions in Functional tests (total memory limit in Functional tests is ~55.24 GiB).
# TSAN will flush shadow memory when reaching this limit.
# It may cause false-negatives, but it's better than OOM.
RUN echo "TSAN_OPTIONS='verbosity=1000 halt_on_error=1 history_size=7 memory_limit_mb=46080'" >> /etc/environment
RUN echo "UBSAN_OPTIONS='print_stacktrace=1'" >> /etc/environment
RUN echo "MSAN_OPTIONS='abort_on_error=1 poison_in_dtor=1'" >> /etc/environment
RUN echo "LSAN_OPTIONS='suppressions=/usr/share/clickhouse-test/config/lsan_suppressions.txt'" >> /etc/environment
# Sanitizer options for current shell (not current, but the one that will be spawned on "docker run")
# (but w/o verbosity for TSAN, otherwise test.reference will not match)
ENV TSAN_OPTIONS='halt_on_error=1 history_size=7 memory_limit_mb=46080'
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
    netbase \
    perl \
    ripgrep \
    llvm-${LLVM_VERSION} \
    moreutils \
    pigz \
    pv \
    brotli \
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

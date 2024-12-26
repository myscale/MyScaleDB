# docker build -t harbor.internal.moqi.ai/mqdb/mqdb-test-sqlancer:3.0.0 .
# docker buildx build --platform linux/amd64,linux/arm64 -t harbor.internal.moqi.ai/mqdb/mqdb-test-sqlancer:3.0.0 . --push
FROM harbor.internal.moqi.ai/mqdb/mqdb-test-base:3.0.0

RUN apt-get update --yes \
    && env DEBIAN_FRONTEND=noninteractive apt-get install maven --yes --no-install-recommends  \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# We need to get the repository's HEAD each time despite, so we invalidate layers' cache
ARG CACHE_INVALIDATOR=0
RUN mkdir /sqlancer && \
	wget -q -O- https://github.com/sqlancer/sqlancer/archive/main.tar.gz | \
		tar zx -C /sqlancer && \
	cd /sqlancer/sqlancer-main && \
	mvn --no-transfer-progress package -DskipTests && \
	rm -r /root/.m2

COPY process_sqlancer_result.py /

SHELL ["/bin/bash", "-c"]
CMD bash

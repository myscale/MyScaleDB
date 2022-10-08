# docker build -t clickhouse/integration-tests-runner .
FROM mqdb-test-integration:1.1

# ARG for quick switch to a given ubuntu mirror
# ARG apt_archive="http://archive.ubuntu.com"
# RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list


COPY modprobe.sh /usr/local/bin/modprobe
COPY dockerd-entrypoint.sh /usr/local/bin/
COPY compose/ /compose/
COPY misc/ /misc/


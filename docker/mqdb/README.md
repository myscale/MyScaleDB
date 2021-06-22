# MQDB

## 镜像

从 Registry 拉取镜像:

```bash
# docker pull harbor.internal.moqi.ai/mqdb/mqdb:<version>-<commit>
```

或者保存为文件上传之后再导入:

```bash
# docker save -o mqdb-image.tgz harbor.internal.moqi.ai/mqdb/mqdb:<version>-<commit>
# docker load -i mqdb-image.tgz
```

## 运行

运行 server

```bash
# docker run -d --name <name> --hostname <hostname> --restart always --ulimit nofile=262144:262144 harbor.internal.moqi.ai/mqdb/mqdb:<version>-<commit>
```

连接到 server

```bash
# docker exec -it <name> bash
# source /opt/intel/oneapi/mkl/$INTEL_ONEAPI_VERSION/env/vars.sh
# clickhouse-client
ClickHouse client version 21.11.5.1.
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 21.11.5 revision 54450.

mqdb :) show databases;

SHOW DATABASES

Query id: ed46c54e-0bdb-45f3-ae38-7ad08c118fe7

┌─name───────────────┐
│ INFORMATION_SCHEMA │
│ default            │
│ information_schema │
│ system             │
└────────────────────┘

4 rows in set. Elapsed: 0.003 sec.
```

存储目录位于 `/var/lib/clickhouse`, 配置文件位置如下:

```text
/etc/clickhouse-server
├── config.d
│   ├── listen.xml
│   └── prometheus.xml
├── config.xml
└── users.xml
```

默认情况下, 会创建三个匿名存储卷持久化配置和数据: `/var/lib/clickhouse`, `/etc/clickhouse-server`, `/etc/clickhouse-client/`, 可以按需挂载目录或者配置文件

如果需要在运行之前初始化或者导入数据, 则可以将相应脚本和数据挂载于 `/docker-entrypoint-initdb.d` 目录下, 在运行时会自动导入, 如:

```bash
#!/bin/bash
set -e

clickhouse client -n <<-EOSQL
    CREATE DATABASE docker;
    CREATE TABLE docker.docker (x Int32) ENGINE = Log;
EOSQL
```

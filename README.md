# MyScaleDB

<div align="center">

<a href='https://www.myscale.com/?utm_source=github&utm_medium=myscaledb'>
<img src="docs/myscaledb-logo-with-text.png" alt="MyScale, the SQL Vector Database for Scalable AI" height=100></img>
</a>
<br></br>

[![Official Website](<https://img.shields.io/badge/-Visit%20the%20Official%20Website%20%E2%86%92-rgb(21,204,116)?style=for-the-badge>)](https://www.myscale.com/?utm_source=github&utm_medium=myscaledb_readme)
[![Playground](<https://img.shields.io/badge/-Try%20It%20Online%20%E2%86%92-rgb(84,56,255)?style=for-the-badge>)](https://console.myscale.com/playground/?utm_source=github&utm_medium=myscaledb_readme)

[![LICENSE](https://img.shields.io/badge/License-Apache%202.0-yellow.svg)](https://github.com/myscale/myscaledb/blob/main/LICENSE)
[![Language](https://img.shields.io/badge/Language-C++20-blue.svg)](https://isocpp.org/)

*Enable every developer to build production-grade GenAI applications with powerful and familiar SQL.*
</div>

## What is MyScaleDB?

**MyScaleDB** is an open-source cloud-native SQL vector database optimized for AI applications and solutions, built on the open-source **ClickHouse** database, allowing us to effectively manage massive volumes of data for the development of robust and scalable AI applications. Some of the most significant benefits of using MyScale include:

* **Built for AI applications:** Manages and supports search and analytical processing of structured and vectorized data on a single platform.
* **Built for performance and scalability:** Cutting-edge OLAP database architecture combined with advanced vector algorithms to perform operations on vectorized data at incredible speeds and scalability.
* **Built for universal accessibility**: SQL with vector-related functions is the only programming language needed to interact with MyScale.

Compared with the customized APIs of specialzied vector databases, MyScale is [more powerful, performant and cost-effective](https://myscale.com/blog/myscale-outperform-specialized-vectordb/) yet simpler to use; thus, suitable for a large community of programmers. Compared with integrated vector databases such as PostgreSQL with pgvector and ElasticSearch with vector extension, MyScale consumes much lower resources and [achieves much better accuracy/speed at structured and vector joint queries](https://myscale.com/blog/myscale-vs-postgres-opensearch/), such as filtered search.

Last but not least, with MyScale's SQL support and [rich data types and functions](https://myscale.com/docs/en/functions/), you can seamlessly manage and query multiple data modalities in a unified system, allowing you to leverage structured, vector, text, time-series and more data types simultaneously with a single SQL query. This streamlined approach ensures rapid and efficient processing, saving time and reducing complexity, empowering you to tackle AI/LLM and big data tasks with ease.

## Why MyScaleDB

* Unified unstructured data and structured data management
* SQL vector database
* Millisecond search on billion vectors
* Highly reliable & linearly scalable
* Hybrid search & complex SQL vector queries
* Support disk-based vector index for high data density[^1]

See MyScale [documentation](https://myscale.com/docs/en/) and [blogs](https://myscale.com/blog/) for more about MyScale’s unique features and advantages. Our [open-source benchmark](https://myscale.github.io/benchmark/) provides detailed comparison with other vector database products.

## Why build MyScale on top of ClickHouse?

[ClickHouse](https://github.com/ClickHouse/ClickHouse) is a popular open-source analytical database that excels at big data processing and analytics due to its columnar storage with advanced compression, skip indexing, and SIMD processing. Unlike transactional databases like PostgreSQL and MySQL, which use row storage and main optimzies for transactional processing, ClickHouse has significantly faster analytical and data scanning speeds.

One of the key operations in combining structured and vector search is filtered search, which involves filtering by other attributes first and then performing vector search on the remaining data. [Columnar storage and pre-filtering are crucial](https://myscale.com/blog/filtered-vector-search-in-myscale/#behind-the-scene) for ensuring high accuracy and high performance in filtered search, which is why we chose to build MyScale on top of ClickHouse.

While we have modified ClickHouse's execution and storage engine in many ways to ensure fast and cost-effective SQL vector queries, many of the features ([#37893](https://github.com/ClickHouse/ClickHouse/issues/56728), [#38048](https://github.com/ClickHouse/ClickHouse/pull/38048), [#37859](https://github.com/ClickHouse/ClickHouse/issues/37859), [#56728](https://github.com/ClickHouse/ClickHouse/issues/56728)) related to general SQL processing have been contributed back to the ClickHouse open source community.

## Creating a MyScaleDB Instance

### MyScaleDB Cloud

The simplest way to use MyScaleDB is to start an instance on MyScale Cloud service. We offer a free pod supporting 5M 768D vectors. Sign up [here](https://myscale.com/) and checkout [MyScale QuickStart](https://myscale.com/docs/en/quickstart/) for more instructions.

### Self-Hosted

#### MyScaleDB Docker Image

To quickly get a MyScaleDB instance up and running, simply pull and run the latest Docker image:

```bash
docker run --name myscaledb myscale/myscaledb:1.4
```

This will start a MyScaleDB instance with default user `default` and no password. You can then connect to the database using `clickhouse-client`:

```bash
docker exec -it myscaledb bash
clickhouse-client
```

To install MyScaleDB locally or on-premise, we recommend using our `docker-compose.yml` file as below. Alternatively, you can customize the configuration file of MyScaleDB.

```yaml
version: '3.7'

services:
  myscaledb:
    image: myscale/myscaledb:1.4
    tty: true
    ports:
      - '8123:8123'
      - '9000:9000'
      - '8998:8998'
      - '9363:9363'
      - '9116:9116'
    volumes:
      - ${DOCKER_VOLUME_DIRECTORY:-.}/volumes/data:/var/lib/clickhouse
      - ${DOCKER_VOLUME_DIRECTORY:-.}/volumes/log:/var/log/clickhouse-server
    deploy:
      resources:
        limits:
          cpus: "16.00"
          memory: 32Gb
```

If you want to customize the configuration file of MyScaleDB, please first copy the `/etc/clickhouse-server` directory from the `myscaledb` container to a directory on your local physical machine. Assuming you have copied it to the `volumes/config` directory, you can freely modify the configuration within the `volumes/config` directory. Then, you just need to add a directory mapping in the above-mentioned `docker-compose.yaml` file to make the configuration take effect.
Here is the mapping of this configuration file:

```yaml
- ${DOCKER_VOLUME_DIRECTORY:-.}/volumes/config:/etc/clickhouse-server
```

## Tutorial

Please refer to our [guide](https://myscale.com/docs/en/vector-search/) for how to create a SQL table with vector index and perform vector search. It's recommended to specify `TYPE SCANN` when creating a vector index in open source MyScaleDB.

## Community

You're welcome to become a part of the groups or channels listed below to engage in discussions, pose queries regarding MyScaleDB, and stay updated on the newest developments related to MyScaleDB:

* Seek help when you use MyScaleDB: [Discord](https://discord.gg/D2qpkqc4Jq)

* Get the latest MyScaleDB news or updates

  * Follow [@MyScaleDB](https://twitter.com/MyScaleDB) on Twitter
  * Follow [@MyScale](https://www.linkedin.com/company/myscale/) on LinkedIn
  * Read [MyScale Blog](https://myscale.com/blog/)

For support, please [contact us](https://myscale.com/contact/).

## Roadmap

* [ ] Inverted index & performant keyword/vector hybrid search
* [ ] Support more storage engines, e.g. `ReplacingMergeTree`
* [ ] LLM observability with MyScale
* [ ] Data-centric LLM

[^1]: The disk-based MSTG (Multi-scale Tree Graph) algorithm is available through [MyScale Cloud](myscale.com), achieving high data density and better indexing & search performance on billion-scale vector data.

## License

MyScaleDB is licensed under the Apache License, Version 2.0. View a copy of the [License file](https://github.com/myscale/myscaledb?tab=License-1-ov-file).

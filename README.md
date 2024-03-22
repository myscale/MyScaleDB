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
docker exec -it myscaledb clickhouse-client
```

To install MyScaleDB locally or on-premise, we recommend using Docker Compose to deploy MyScaleDB. Below is the recommended directory structure and the location of the `docker-compose.yaml` file.

```bash
❯ tree myscaledb    
myscaledb
├── docker-compose.yaml
└── volumes

1 directory, 1 file
```

After familiarizing yourself with the directory structure and the location of the `docker-compose.yaml` file, the next step is to define the configuration for your deployment. We recommend starting with the following configuration in your `docker-compose.yaml` file, which you can adjust based on your specific requirements:

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

Use the following command to get it running:

```bash
cd myscaledb
docker compose up -d
```

You can access the myscaledb command line interface using the following command. From now on, you are free to execute SQL statements.

```bash
docker exec -it myscaledb-myscaledb-1 clickhouse-client
```

If you want to customize the configuration file of MyScaleDB, please first copy the `/etc/clickhouse-server` directory from your `myscaledb` container to a directory on your local physical machine. Assuming you have copied it to the `volumes/config` directory, you can freely modify the configuration within the `volumes/config` directory. Then, you just need to add a directory mapping in the above-mentioned `docker-compose.yaml` file to make the configuration take effect.
Here is the mapping of this configuration file:

```yaml
- ${DOCKER_VOLUME_DIRECTORY:-.}/volumes/config:/etc/clickhouse-server
```

## Tutorial

Please refer to our [guide](https://myscale.com/docs/en/vector-search/) for how to create a SQL table with vector index and perform vector search. It's recommended to specify `TYPE SCANN` when creating a vector index in open source MyScaleDB.

### Create a Table in MyScaleDB

```sql
CREATE TABLE default.wiki_abstract
(
    `id` UInt64,
    `body` String,
    `title` String,
    `url` String,
    `body_vector` Array(Float32),
    CONSTRAINT check_length CHECK length(body_vector) = 384
)
ENGINE = MergeTree
ORDER BY id;
```

### Insert Data to Your Table

```sql
INSERT INTO default.wiki_abstract SELECT * FROM s3('https://myscale-datasets.s3.ap-southeast-1.amazonaws.com/wiki_abstract_with_vector.parquet','Parquet');

OPTIMIZE TABLE default.wiki_abstract FINAL;
```

### Create the Vector Index

```sql
ALTER TABLE default.wiki_abstract ADD VECTOR INDEX vec_idx body_vector TYPE SCANN('metric_type=Cosine');

-- We can query the index build progress from the `vector_indices` table and wait until the progress becomes `Built`, indicating that the index creation is complete.
SELECT * FROM system.vector_indices;
```

### Execute Vector Search

```sql
SELECT id, title, distance(body_vector, [-0.051993933,-0.014571957,-0.067694284,-0.025587771,-0.039530866,-0.038116932,-0.025040321,0.09107845,-0.042887665,-0.05916685,0.0016676356,-0.03583563,-0.046406135,-0.018933294,-0.019242926,0.05435089,-0.0022332764,-0.029247247,-0.047381226,-0.028552702,0.074579544,-0.013048853,-0.02171472,-0.024649369,-0.016868504,0.049497478,-0.094687544,0.013853871,0.044450607,-0.026169028,-0.004910583,0.050561734,0.003975122,0.027577253,0.0063147848,-0.064254835,0.00589612,-0.022918286,-0.03153694,0.054887727,0.14274344,0.0079141045,0.010968703,-0.0035762321,-0.061702404,0.015499965,-0.060662124,0.025801793,-0.020549921,0.00081662735,-0.05465167,0.03287687,-0.05216912,-0.034720425,0.09211666,0.01391376,-0.013022239,0.07159377,-0.016452607,0.025693206,-0.007101831,0.008424894,-0.0653154,0.009117553,0.054408558,-0.019227771,-0.016855529,-0.0017008693,-0.030391308,0.042674664,-0.03890756,0.09212165,-0.062187918,-0.019561214,0.0025066605,0.021409642,0.025898892,-0.049299367,-0.021074196,-0.1189605,-0.07355872,-0.15445043,-0.057826668,-0.014547524,0.013788045,0.047765154,-0.045130353,-0.033162475,0.0798823,0.00014898424,-0.07370452,0.042748183,0.051670443,0.010215425,0.038576707,0.02325246,0.042541727,-0.027946398,-0.052863315,0.07435391,-0.030535538,-0.026040124,0.12288541,-0.0020252853,0.0037650217,-0.049053755,0.035206515,0.002714659,-0.055957433,-0.1043515,0.12296011,-0.018407518,0.11477282,-0.018928673,0.041221637,-0.034674577,-0.05691061,-0.011879819,0.009807924,-0.0016493463,0.045088265,0.027347907,0.04364125,0.008214966,0.016615324,-0.098858,0.074695654,-1.9762962e-33,0.030646795,-0.07171656,-0.0070392075,0.06650123,0.045193307,0.012263713,-0.023790587,0.051231034,-0.011579724,0.051707372,0.02878731,-0.001285007,0.017610451,0.07617186,0.12837116,-0.030996455,0.08911589,-0.028641228,0.013207627,0.0029564973,0.043276913,0.010178338,-0.02085952,-0.04588205,-0.031221278,-0.0387124,0.020109951,-0.02699747,0.024315225,0.07131897,0.035871625,-0.06737809,-0.07465318,-0.014699111,0.048917893,-0.009248528,-0.018016934,0.02356299,0.037189223,-0.0070787882,-0.051331587,-0.039600756,-0.031648707,-0.029654862,-0.038506385,-0.061981704,0.04653717,0.05385589,-0.032988995,0.06430667,0.06100978,0.0062337937,0.02448809,0.08676914,0.052251935,-0.025303239,0.015694777,0.02655673,0.012427463,0.1381833,-0.010661728,0.08350895,-0.105721444,-0.018787563,-0.07858833,0.056970757,0.07071342,-0.018518107,0.070829704,0.018870745,-0.03736615,-0.048424892,0.008875318,0.024713999,0.02551521,-0.011756489,0.073875934,0.0114348745,-0.04483199,-0.016004276,-0.08356264,0.010703036,0.0066822283,-0.05353009,-0.018569078,-0.004168434,0.05815997,-0.07308629,-0.059285823,0.029934714,0.0003925204,-0.029858263,0.012823868,-0.054889712,0.04934564,7.279117e-34,-0.041897744,0.054874696,-0.031515755,0.10118687,0.0458668,-0.06283181,0.04172824,-0.0153129045,0.047121603,-0.03010246,-0.061485767,0.013687931,-0.021871995,0.073475316,0.0829944,0.011405637,-0.032571495,-0.027177166,0.0642351,-0.020283077,0.05567658,-0.057863697,0.08828164,0.07192206,0.0007150115,0.059799276,-0.043101445,-0.018918894,-0.059285942,-0.033429902,0.01998538,-0.03714634,-0.04410662,0.040724766,-0.08054264,0.0058395006,0.10387117,0.053397756,0.04949688,-0.032475222,0.07815859,-0.040285874,0.010772476,-0.006843925,-0.052518513,0.08010702,0.02556263,-0.018286457,-0.06187721,-0.006310011,-0.06052168,0.03772284,-0.028093407,-0.00971233,-0.0028837582,-0.10602951,0.046458527,-0.0033366857,-0.030772595,0.03566498,0.015609401,-0.04063339,-0.030757135,0.0012650898,0.04576847,0.023140637,0.020672783,-0.082763225,-0.057267062,0.02977531,-0.038069632,0.093539745,-0.049752194,-0.09787799,-0.14522925,0.083466195,-0.097279325,-0.017175317,0.00025348098,0.09004034,-0.09311094,-0.025154117,0.007995562,-0.044069506,-0.09381035,-0.002082798,0.088507004,0.008801484,0.0034103142,-0.004868861,0.02166316,0.0583604,-0.012032157,0.05901603,0.014640149,-1.3841484e-8,-0.004460958,0.066267215,0.0016688921,0.0015052046,0.05686885,-0.008889378,-0.023157073,0.006498412,0.020371925,-0.025288232,0.11189946,-0.035977192,0.012520477,0.053084545,0.058390282,-0.010065113,-0.05930951,-0.057696298,-0.0655608,-0.03956036,0.052485723,-0.0059757195,-0.014863029,0.0030453473,-0.10093801,-0.02814902,0.031147273,-0.008842873,0.044070948,-0.0055524884,0.07154635,0.051045228,0.02193814,-0.0027840915,0.0294103,-0.09688244,-0.08516746,0.03036262,0.037378754,0.10777078,-0.05593432,0.08047652,-0.046416387,0.036919285,0.08739361,-0.025080625,0.007479923,-0.05024608,-0.018117117,-0.10586108,0.011069658,0.089402206,0.002131186,0.08381613,0.049733363,-0.018331422,0.024617516,-0.0040164976,-0.0828176,0.059984796,-0.116080105,-0.03668811,0.04750555,0.031703997]) AS distance FROM default.wiki_abstract ORDER BY distance ASC LIMIT 5;
```

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

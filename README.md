<h1 align="center" style="border-bottom: none">
    <a href="https://datazip.io/olake" target="_blank">
        <img alt="olake" src="https://github.com/user-attachments/assets/d204f25f-5289-423c-b3f2-44b2194bdeaf" width="100" height="100"/>
    </a>
    <br>OLake
</h1>

<p align="center">The fastest open-source tool for replicating databases to Apache Iceberg. OLake, an easy-to-use web interface and a CLI for efficient, scalable, & real-time data ingestion. Visit <a href="https://olake.io/" target="_blank">olake.io/docs</a> for the full documentation, and benchmarks</p>

<p align="center">
    <a href="https://github.com/datazip-inc/olake/issues"><img alt="GitHub issues" src="https://img.shields.io/github/issues/datazip-inc/olake"/></a> <a href="https://olake.io/docs"><img alt="Documentation" height="22" src="https://img.shields.io/badge/view-Documentation-blue?style=for-the-badge"/></a>
    <a href="https://join.slack.com/t/getolake/shared_invite/zt-2utw44do6-g4XuKKeqBghBMy2~LcJ4ag"><img alt="slack" src="https://img.shields.io/badge/Join%20Our%20Community-Slack-blue"/></a>
</p>

## 🧊 OLake — Super-fast Sync from Databases to Apache Iceberg

> **OLake** is an open-source connector for replicating data from transactional databases like **PostgreSQL, MySQL, MongoDB Oracle & Kafka** to open data lakehouse formats like **Apache Iceberg** — at blazing speeds and minimal infrastructure cost.

---

### 🚀 Why OLake?

- 🧠 **Smart sync**: Full + CDC replication with automatic schema discovery  
- ⚡ **High throughput**: 46K RPS (Postgres), 36K RPS (CDC)  
- 💾 **Iceberg-native**: Supports Glue, Hive, JDBC, REST catalogs  
- 🖥️ **Self-serve UI**: Deploy via Docker Compose and sync in minutes  
- 💸 **Infra-light**: No Spark, no Flink — just Go and Postgres

---

### 📊 Benchmarks


| Source → Destination | Throughput            | Relative Performance        |Full Report        |
|----------------------|-----------------------|-----------------------------|-------------------|
| Postgres → Iceberg   | 46,262 RPS (Full load)| 101× faster than Airbyte    |[Full Report](https://olake.io/docs/connectors/postgres/benchmarks)
| MySQL → Iceberg      | 64,334 RPS (Full load)| 9× faster than Airbyte     |[Full Report](https://olake.io/docs/connectors/mysql/benchmarks)
| MongoDB → Iceberg    | Coming Soon!          | 20× faster than Airbyte     |[Full Report](https://olake.io/docs/connectors/mongodb/benchmarks)
| Oracle → Iceberg     | Coming Soon!          |                             |

**These are preliminary results. Fully reproducible benchmark scores will be published soon.*

---

### 🔧 Supported Sources and Destinations


#### Sources


| Source        | Full Load    |  CDC          | Incremental       | Notes                       |
|---------------|--------------|---------------|-------------------|-----------------------------|
| PostgreSQL    | ✅           | ✅ `wal2json` | WIP                |`pgoutput` support WIP       |
| MySQL         | ✅           | ✅            | WIP                | Binlog-based CDC            |
| MongoDB       | ✅           | ✅            | WIP                | Oplog-based CDC             |
| Oracle        | ✅           | Coming Soon!  | WIP                |                             |
| Kafka        | Coming Soon! | Coming Soon!  | WIP                |                            |

| Functionality                 | MongoDB | Postgres | MySQL | Oracle | Kafka |
| :---------------------------- | :-----: | :------: | :---: | :---: | :---: |
| Full Refresh Sync             |    ✅    |    ✅     |   ✅   |   ✅   |  WIP  |
| Incremental Sync              |    WIP    |    WIP     |   WIP   |  WIP   |  WIP   |
| CDC Sync                      |    ✅    |    ✅     |   ✅   |   WIP   |   WIP   |
| Full Load Parallel Processing |    ✅    |    ✅     |   ✅   |  ✅   |   WIP   |
| CDC Parallel Processing       |    ✅    |    ❌     |   ❌   |  WIP   |  WIP   |
| Resumable Full Load           |    ✅    |    ✅     |   ✅   |  ✅   |  ✅   |
| CDC Heartbeat (Planned)                 |    -    |    -     |   -   |  -   |  -   |

1. [Getting started Postgres -> Writers](https://github.com/datazip-inc/olake/tree/master/drivers/postgres) | [Postgres Docs](https://olake.io/docs/connectors/postgres/overview)
2. [Getting started MongoDB -> Writers](https://github.com/datazip-inc/olake/tree/master/drivers/mongodb) | [MongoDB Docs](https://olake.io/docs/connectors/mongodb/overview)
3. [Getting started MySQL -> Writers](https://github.com/datazip-inc/olake/tree/master/drivers/mysql)  | [MySQL Docs](https://olake.io/docs/connectors/mysql/overview)
4. [Getting started Oracle -> Writers](https://github.com/datazip-inc/olake/tree/master/drivers/oracle)  | [MySQL Docs](https://olake.io/docs/connectors/oracle/overview)

#### Destinations


| Destination    | Format    | Supported Catalogs                                            |
|----------------|-----------|---------------------------------------------------------------|
| Iceberg        | ✅         | Glue, Hive, JDBC, REST (Nessie, Polaris, Unity, Lakekeeper)  |
| Parquet        | ✅         | Filesystem                                                   |
| Other formats  | 🔜         | Planned: Delta Lake, Hudi                                    |


| Functionality              | Local Filesystem | AWS S3 | Apache Iceberg |
| :------------------------- | :--------------: | :----: | :------------: |
| Flattening & Normalization |        ✅         |   ✅    |       ✅        |
| Partitioning               |        ✅         |   ✅    |       ✅        |
| Schema Data Type Changes   |        ✅         |   ✅    |      WIP        |
| Schema Evolution           |        ✅         |   ✅    |       ✅        |

##### Writer Docs

1. [Apache Iceberg Docs](https://olake.io/docs/writers/iceberg/overview)
    1. Catalogs
       1. [AWS Glue Catalog](https://olake.io/docs/writers/iceberg/catalog/glue)
       2. [REST Catalog](https://olake.io/docs/writers/iceberg/catalog/rest)
       3. [JDBC Catalog](https://olake.io/docs/writers/iceberg/catalog/jdbc)
       4. [Hive Catalog](https://olake.io/docs/writers/iceberg/catalog/hive)
    2. [Azure ADLS Gen2](https://olake.io/docs/writers/iceberg/azure)
    3. [Google Cloud Storage (GCS)](https://olake.io/docs/writers/iceberg/gcs)
    4. [MinIO (local)](https://olake.io/docs/writers/iceberg/docker-compose#local-catalog-test-setup)
    5. Iceberg Table Management
       1. [S3 Tables Supported](https://olake.io/docs/writers/iceberg/s3-tables)

2. Parquet Writer
   1. [AWS S3 Docs](https://olake.io/docs/writers/parquet/s3)
   2. [Google Cloud Storage (GCS)](https://olake.io/docs/writers/parquet/gcs)
   3. [Local FileSystem Docs](https://olake.io/docs/writers/parquet/local)

---

### 🧪 Quickstart (UI + Docker)

OLake UI is a web-based interface for managing OLake jobs, sources, destinations, and configurations. You can run the entire OLake stack (UI, Backend, and all dependencies) using Docker Compose. This is the recommended way to get started.
Run the UI, connect your source DB, and start syncing in minutes. 

```sh
curl -sSL https://raw.githubusercontent.com/datazip-inc/olake-ui/master/docker-compose.yml | docker compose -f - up -d
```

**Access the UI:**
      * **OLake UI:** [http://localhost:8000](http://localhost:8000)
      * Log in with default credentials: `admin` / `password`.

Detailed getting started using OLake UI can be found [here](https://olake.io/docs/getting-started/olake-ui).

![olake-ui](https://github.com/user-attachments/assets/6081e9ad-7aef-465f-bde1-5b41b19ec6cd)

#### Creating Your First Job

With the UI running, you can create a data pipeline in a few steps:

1. **Create a Job:** Navigate to the **Jobs** tab and click **Create Job**.
2. **Configure Source:** Set up your source connection (e.g., PostgreSQL, MySQL, MongoDB).
3. **Configure Destination:** Set up your destination (e.g., Apache Iceberg with a Glue, REST, Hive, or JDBC catalog).
4. **Select Streams:** Choose which tables to sync and configure their sync mode (`CDC` or `Full Refresh`).
5. **Configure & Run:** Give your job a name, set a schedule, and click **Create Job** to finish.

For a detailed walkthrough, refer to the [Jobs documentation](https://olake.io/docs/jobs/create-jobs).

---

### 🛠️ CLI Usage (Advanced)

For advanced users and automation, OLake's core logic is exposed via a powerful CLI. The core framework handles state management, configuration validation, logging, and type detection. It interacts with drivers using four main commands:

* `spec`: Returns a render-able JSON Schema for a connector's configuration.
* `check`: Validates connection configurations for sources and destinations.
* `discover`: Returns all available streams (e.g., tables) and their schemas from a source.
* `sync`: Executes the data replication job, extracting from the source and writing to the destination.

**Find out more about how OLake works [here](https://olake.io/docs).**

---

#### Install OLake

Below are other different ways you can run OLake:

1. [OLake UI (Recommended)](https://olake.io/docs/getting-started/olake-ui)
2. [Standalone Docker container](https://olake.io/docs/install/docker)
3. [Airflow on EC2](https://olake.io/blog/olake-airflow-on-ec2?utm_source=chatgpt.com)
4. [Airflow on Kubernetes](https://olake.io/blog/olake-airflow)

---

### Playground

1. [OLake + Apache Iceberg + REST Catalog + Presto](https://olake.io/docs/playground/olake-iceberg-presto)
2. [OLake + Apache Iceberg + AWS Glue + Trino](https://olake.io/iceberg/olake-iceberg-trino)
3. [OLake + Apache Iceberg + AWS Glue + Athena](https://olake.io/iceberg/olake-iceberg-athena)
4. [OLake + Apache Iceberg + AWS Glue + Snowflake](https://olake.io/iceberg/olake-glue-snowflake)

---

### 📦 Architecture

![OLake Architecture](https://olake.io/blog/olake-architecture-deep-dive)

- Stateless Go-based CLI & Temporal workers
- Iceberg writers integrated directly with Iceberg format spec
- Connectors modularized for easy plugin-based extension

---

### 🌍 Use Cases

- ✅ Migrate from OLTP to Iceberg without Spark or Flink
- ✅ Enable BI over fresh CDC data using Athena, StarRocks, Trino, Presto, Dremio, Databricks, Snowflake and more!
- ✅ Build real-time data lakes on cloud object stores

---

### 🧭 Roadmap Highlights

- [x] Oracle Full Load Support
- [ ] Oracle Incremental CDC (WIP)
- [ ] Filters for Full Load and Incremental (WIP)
- [ ] Real-time Streaming Mode (Kafka)
- [ ] Iceberg V3 Support

📌 Check out our [GitHub Project Roadmap](https://github.com/orgs/datazip-inc/projects/5) and the [Upcoming OLake Roadmap](https://olake.io/docs/roadmap) to track what's next. If you have ideas or feedback, please share them in our [GitHub Discussions](https://github.com/datazip-inc/olake/discussions) or by opening an issue.

---

### 🤝 Contributing

We ❤️ contributions, big or small!

Check out our [Bounty Program](https://olake.io/docs/community/issues-and-prs#goodies). A huge thanks to all our amazing [contributors!](https://github.com/datazip-inc/olake/graphs/contributors)

* To contribute to the **OLake core**, see [CONTRIBUTING.md](https://www.google.com/search?q=CONTRIBUTING.md).
* To contribute to the **UI**, visit the [OLake UI Repository](https://github.com/datazip-inc/olake-ui).
* To contribute to our **website and documentation**, visit the [Olake Docs Repository](https://github.com/datazip-inc/olake-docs/).


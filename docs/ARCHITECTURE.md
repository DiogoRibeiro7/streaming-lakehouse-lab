# Architecture

This document describes the architecture of the Streaming Lakehouse Lab, including system components, data flows, and service responsibilities.

## Table of Contents

- [System Overview](#system-overview)
- [Component Diagram](#component-diagram)
- [Service Responsibilities](#service-responsibilities)
- [Data Flows](#data-flows)
- [Network Architecture](#network-architecture)
- [Storage Architecture](#storage-architecture)

## System Overview

The Streaming Lakehouse Lab is a production-quality reference architecture demonstrating real-time data processing with Apache Flink, Apache Kafka, and Apache Iceberg. It combines streaming and batch processing paradigms to enable low-latency analytics on continuously updating datasets.

**Core Capabilities:**
- Real-time event processing with Flink
- Exactly-once processing semantics
- ACID transactions on data lake (Iceberg)
- Time travel and schema evolution
- Unified batch and streaming queries

## Component Diagram

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          STREAMING LAKEHOUSE LAB                              │
└──────────────────────────────────────────────────────────────────────────────┘

                                  ┌─────────────┐
                                  │   Clients   │
                                  │  (Python,   │
                                  │   Java)     │
                                  └──────┬──────┘
                                         │
                    ┌────────────────────┼────────────────────┐
                    │                    │                    │
                    ▼                    ▼                    ▼
          ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
          │  Kafka Cluster  │  │ Flink JobManager│  │   SQL Gateway   │
          │   (KRaft Mode)  │  │    :8081        │  │     :8083       │
          │     :9092       │  └────────┬────────┘  └─────────────────┘
          └────────┬────────┘           │
                   │                    │
                   │         ┌──────────┼──────────┐
                   │         │                     │
                   │         ▼                     ▼
                   │  ┌─────────────────┐  ┌─────────────────┐
                   │  │ Flink TaskMgr 1 │  │ Flink TaskMgr 2 │
                   │  │  (Workers)      │  │  (Workers)      │
                   │  └─────────────────┘  └─────────────────┘
                   │
                   │  ┌──────────────────────────────────────────────┐
                   │  │         Flink Processing Layer              │
                   │  │  ┌────────────┐  ┌────────────┐            │
                   └──┼─▶│  Kafka →   │  │  CEP       │            │
                      │  │  Iceberg   │  │  Patterns  │            │
                      │  └────────────┘  └────────────┘            │
                      │  ┌────────────┐  ┌────────────┐            │
                      │  │  Window    │  │  Stateful  │            │
                      │  │  Agg       │  │  Dedup     │            │
                      │  └────────────┘  └────────────┘            │
                      └──────────────────┬───────────────────────────┘
                                         │
                                         ▼
                      ┌──────────────────────────────────────┐
                      │      Iceberg REST Catalog            │
                      │            :8181                     │
                      └──────────────┬───────────────────────┘
                                     │
                      ┌──────────────┼───────────────────────┐
                      │              │                       │
                      ▼              ▼                       ▼
             ┌───────────────┐ ┌──────────────┐   ┌────────────────┐
             │  PostgreSQL   │ │    MinIO     │   │  Data Files    │
             │   Catalog     │ │  S3 Storage  │   │   (Parquet)    │
             │    :5432      │ │    :9000     │   │  in MinIO      │
             └───────────────┘ └──────────────┘   └────────────────┘
                  (Metadata)      (Object Store)      (Data Lake)
```

## Service Responsibilities

### Apache Kafka (Confluent Platform)

**Role:** Event streaming platform and message broker

**Responsibilities:**
- Ingest raw events from producers
- Provide durable, ordered event log
- Enable pub/sub messaging patterns
- Support exactly-once semantics
- Serve as source for Flink streaming jobs

**Configuration:**
- **Port:** 9092 (plaintext)
- **Mode:** KRaft (no ZooKeeper)
- **Topics:** Auto-created or via `make topics`
- **Partitions:** 3 (default for parallelism)
- **Replication:** 1 (single-node setup)

**Key Topics:**
- `sensors` - IoT sensor readings
- `events-raw` - Raw event stream
- `events-deduped` - Deduplicated events
- `sensors-alerts` - Anomaly detection alerts
- `door_sessions` - CEP pattern matches

---

### Apache Flink (Cluster Mode)

**Role:** Distributed stream and batch processing engine

**Components:**

#### 1. JobManager (:8081)
- Coordinates job execution
- Manages checkpoints and savepoints
- Schedules tasks to TaskManagers
- Provides Web UI for monitoring

#### 2. TaskManagers (Workers)
- Execute job tasks (sources, operators, sinks)
- Maintain operator state
- Handle data shuffling between operators
- Report metrics to JobManager

#### 3. SQL Gateway (:8083)
- Provides REST API for SQL queries
- Enables programmatic job submission
- Supports Flink SQL syntax
- Integrates with Iceberg catalog

**Responsibilities:**
- Execute streaming and batch jobs
- Manage stateful computations with RocksDB
- Provide exactly-once processing guarantees
- Checkpoint state to fault-tolerant storage
- Support event-time processing with watermarks

**Configuration:**
- **JobManager Port:** 8081 (Web UI)
- **SQL Gateway Port:** 8083 (REST API)
- **Parallelism:** Configurable per job
- **Checkpointing:** Enabled with configurable interval
- **State Backend:** RocksDB (for large state)

---

### Apache Iceberg

**Role:** Open table format for huge analytic datasets

**Responsibilities:**
- Provide ACID transactions on data lake
- Enable schema evolution without downtime
- Support time travel queries
- Optimize query performance with hidden partitioning
- Manage metadata for versioned tables

**Components:**

#### 1. REST Catalog (:8181)
- Centralized metadata service
- Multi-table namespace management
- Atomic table operations
- Integration with PostgreSQL for persistence

#### 2. Table Format
- Snapshot isolation
- Partition evolution
- Metadata optimization
- File pruning

**Configuration:**
- **Catalog Type:** REST
- **Catalog URI:** http://iceberg-rest:8181
- **Warehouse Location:** s3a://lakehouse/warehouse
- **Metadata Backend:** PostgreSQL
- **File Format:** Parquet (default)

---

### PostgreSQL

**Role:** Relational database for Iceberg catalog metadata

**Responsibilities:**
- Store Iceberg table metadata
- Track table snapshots and versions
- Manage schema definitions
- Provide ACID guarantees for catalog operations

**Configuration:**
- **Port:** 5432
- **Database:** iceberg_catalog
- **User:** iceberg
- **Schema:** Initialized by `docker/postgres/init-iceberg-catalog.sql`

**Tables:**
- `iceberg_tables` - Table registry
- `iceberg_namespace_properties` - Namespace configuration
- `iceberg_snapshots` - Snapshot history

---

### MinIO

**Role:** S3-compatible object storage for data lake files

**Responsibilities:**
- Store Iceberg data files (Parquet)
- Store Iceberg metadata files (Avro)
- Provide S3 API compatibility
- Enable local development without AWS

**Configuration:**
- **API Port:** 9000 (S3 compatible)
- **Console Port:** 9001 (Web UI)
- **Access Key:** admin
- **Secret Key:** password123
- **Bucket:** lakehouse

**Storage Layout:**
```
lakehouse/
└── warehouse/
    └── streaming_lakehouse/        # Namespace
        ├── events_iceberg/          # Table
        │   ├── metadata/            # Table metadata
        │   │   ├── v1.metadata.json
        │   │   └── snap-*.avro
        │   └── data/                # Data files
        │       └── date_partition=2024-01-13/
        │           └── *.parquet
        └── sensors/
            └── ...
```

---

## Data Flows

### Flow 1: Real-Time Event Ingestion

```
Producer (Python/Java)
       │
       │ JSON events
       ▼
   Kafka Topic
   (sensors)
       │
       │ Consume
       ▼
  Flink Job
  (KafkaToIcebergJob)
       │
       │ Write batch
       ▼
  Iceberg Table
  (events_iceberg)
       │
       │ Store files
       ▼
MinIO (S3)
  + PostgreSQL (metadata)
```

**Characteristics:**
- **Latency:** Sub-second ingestion to Kafka
- **Throughput:** Thousands of events/sec
- **Guarantees:** Exactly-once semantics end-to-end
- **Format Conversion:** JSON → Parquet
- **Partitioning:** By date (yyyy-MM-dd)

---

### Flow 2: Stateful Deduplication

```
Raw Events
    │
    ▼
Kafka (events-raw)
    │
    ▼
Flink Job (StatefulDedupJob)
    │
    ├─ Key by event_id
    │
    ├─ Check ValueState
    │  ├─ First occurrence → Keep
    │  └─ Duplicate → Filter
    │
    ├─ Update state with TTL
    │
    ▼
Kafka (events-deduped)
```

**Characteristics:**
- **State:** Keyed ValueState with 10-min TTL
- **Cleanup:** Incremental background cleanup
- **Latency:** Milliseconds per event
- **Memory:** Bounded by TTL window

---

### Flow 3: CEP Pattern Detection

```
Sensor Events
    │
    ▼
Kafka (sensors)
    │
    ▼
Flink CEP Job (sensor_cep)
    │
    ├─ Key by device_id
    │
    ├─ Pattern: OPEN → CLOSE (within 2 min)
    │
    ├─ Watermarks (60s lateness)
    │
    ├─ Match → Create session
    │
    ▼
Kafka (door_sessions)
```

**Characteristics:**
- **Pattern Window:** 2 minutes
- **Lateness:** 60 seconds allowed
- **Output:** Session duration and events
- **Use Case:** Door open/close tracking

---

### Flow 4: Window Aggregation

```
Streaming Events
    │
    ▼
Kafka Topic
    │
    ▼
Flink Windowing Job
    │
    ├─ Tumbling Window (5 min)
    │
    ├─ Aggregate (count, sum, avg)
    │
    ├─ Key by dimension
    │
    ▼
Iceberg Table (aggregates)
```

**Characteristics:**
- **Window Type:** Tumbling (non-overlapping)
- **Window Size:** 5 minutes
- **Trigger:** On watermark
- **Output:** Pre-aggregated metrics

---

### Flow 5: EWMA Anomaly Detection

```
Sensor Readings
    │
    ▼
Kafka (sensors)
    │
    ▼
EWMA Job (ewma_ad.py)
    │
    ├─ Key by device_id
    │
    ├─ Calculate EWMA & variance
    │  EWMA_t = α·x_t + (1-α)·EWMA_{t-1}
    │
    ├─ Detect: |x - EWMA| > K·σ
    │
    ├─ Update state
    │
    ▼
Kafka (sensors-alerts)
```

**Characteristics:**
- **Algorithm:** Exponentially Weighted Moving Average
- **Parameters:** α=0.3, K=3.0 (configurable)
- **State:** Per-device EWMA and variance
- **Output:** Anomaly alerts with metrics

---

## Network Architecture

### Internal Network (Docker Compose)

```
┌─────────────────────────────────────────────────────────┐
│  Docker Network: streaming-lakehouse-net                │
│                                                          │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐       │
│  │   kafka    │  │   flink-   │  │ iceberg-   │       │
│  │  :9092     │  │ jobmanager │  │   rest     │       │
│  └────────────┘  │   :8081    │  │   :8181    │       │
│                  └────────────┘  └────────────┘       │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐       │
│  │ postgres   │  │   minio    │  │   flink-   │       │
│  │  :5432     │  │ :9000,:9001│  │ taskmanager│       │
│  └────────────┘  └────────────┘  └────────────┘       │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

**Service DNS Resolution:**
- Services communicate using hostnames (e.g., `kafka:9092`)
- Docker Compose DNS resolves service names automatically
- No external DNS or service discovery needed

**Port Mapping (Host:Container):**
- `9092:9092` - Kafka
- `5432:5432` - PostgreSQL
- `9000:9000` - MinIO API
- `9001:9001` - MinIO Console
- `8081:8081` - Flink JobManager UI
- `8083:8083` - Flink SQL Gateway
- `8181:8181` - Iceberg REST Catalog

---

## Storage Architecture

### Object Storage Hierarchy (MinIO)

```
s3a://lakehouse/
├── warehouse/                           # Iceberg warehouse root
│   └── streaming_lakehouse/             # Namespace/database
│       ├── events_iceberg/              # Table
│       │   ├── metadata/
│       │   │   ├── v1.metadata.json     # Table metadata
│       │   │   ├── v2.metadata.json
│       │   │   ├── snap-1.avro          # Snapshot manifests
│       │   │   └── ...
│       │   └── data/
│       │       ├── date_partition=2024-01-13/
│       │       │   ├── 00000-0-*.parquet
│       │       │   └── 00001-0-*.parquet
│       │       └── date_partition=2024-01-14/
│       │           └── ...
│       └── sensors/
│           └── ...
└── temp/                                 # Temporary staging area
```

### Catalog Metadata (PostgreSQL)

```
iceberg_catalog (database)
├── iceberg_tables
│   ├── catalog_name
│   ├── table_namespace
│   ├── table_name
│   ├── metadata_location        # Pointer to current metadata
│   └── previous_metadata_location
│
├── iceberg_namespace_properties
│   ├── namespace
│   └── properties
│
└── iceberg_snapshots
    ├── snapshot_id
    ├── parent_snapshot_id
    └── manifest_list_location
```

---

## Processing Patterns

### 1. Lambda Architecture (Batch + Stream)

```
          ┌─────────────────┐
          │  Raw Data Lake  │
          │   (Iceberg)     │
          └────────┬────────┘
                   │
         ┌─────────┼─────────┐
         │                   │
         ▼                   ▼
   ┌──────────┐      ┌──────────────┐
   │  Batch   │      │  Streaming   │
   │  Layer   │      │    Layer     │
   │ (Flink)  │      │   (Flink)    │
   └────┬─────┘      └──────┬───────┘
        │                   │
        └─────────┬─────────┘
                  │
                  ▼
          ┌──────────────┐
          │ Serving Layer│
          │  (Iceberg)   │
          └──────────────┘
```

### 2. Kappa Architecture (Stream-Only)

```
Kafka Events → Flink Stream Processing → Iceberg Tables
                     ↓
              State Management
              (RocksDB + TTL)
```

---

## Deployment Models

### Local Development (Docker Compose)
- Single-node deployment
- All services on localhost
- Suitable for development and testing
- Resource requirements: 8GB RAM minimum

### Production Considerations
- **Kafka:** Multi-broker cluster with replication
- **Flink:** Separate JobManager/TaskManager nodes
- **Iceberg:** S3/HDFS for warehouse, RDS for catalog
- **High Availability:** Multiple replicas, load balancing
- **Monitoring:** Prometheus, Grafana integration

---

## Security Architecture

### Current Configuration (Development)

**Authentication:**
- MinIO: Static credentials (admin/password123)
- PostgreSQL: Password authentication
- Kafka: PLAINTEXT (no auth)

**Network:**
- Internal Docker network isolation
- No TLS/SSL encryption

### Production Recommendations

**Authentication:**
- Kafka: SASL/SCRAM or SASL/PLAIN
- MinIO: IAM roles, temporary credentials
- PostgreSQL: Certificate-based auth
- Flink: Kerberos integration

**Encryption:**
- Kafka: SSL/TLS for wire encryption
- MinIO: HTTPS endpoints
- PostgreSQL: SSL connections
- At-rest encryption for data files

**Authorization:**
- Kafka ACLs for topic access
- MinIO bucket policies
- Iceberg table-level permissions
- Network policies for pod communication

---

## Scalability Considerations

### Horizontal Scaling

**Kafka:**
- Add brokers to cluster
- Increase topic partitions
- Replication factor ≥ 3

**Flink:**
- Add TaskManager nodes
- Increase job parallelism
- Scale state backend storage

**MinIO:**
- Add nodes to erasure coding set
- Distribute buckets across nodes
- Use distributed mode

### Vertical Scaling

**Memory:**
- Flink TaskManager heap size
- RocksDB block cache
- Kafka page cache

**CPU:**
- Flink operator parallelism
- Kafka broker threads
- Compaction workers

**Storage:**
- MinIO disk arrays
- Iceberg compaction frequency
- Snapshot retention policy

---

## Monitoring and Observability

### Metrics Collection

```
Flink Metrics → Prometheus → Grafana
     │
     ├─ Job metrics (records/sec, latency)
     ├─ Checkpoint metrics (duration, size)
     ├─ State metrics (size, access rate)
     └─ Resource metrics (CPU, memory)

Kafka Metrics → JMX Exporter → Prometheus
     │
     ├─ Broker metrics (throughput, lag)
     ├─ Topic metrics (size, retention)
     └─ Consumer group lag

Iceberg Metrics → Custom Exporter
     │
     ├─ Table size
     ├─ Snapshot count
     └─ Query latency
```

### Health Checks

See `make health` for automated service health checks.

---

## References

- [Apache Flink Documentation](https://flink.apache.org/docs)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation)
- [Apache Iceberg Specification](https://iceberg.apache.org/spec)
- [MinIO Documentation](https://min.io/docs)
- [Flink + Iceberg Integration](https://iceberg.apache.org/flink)

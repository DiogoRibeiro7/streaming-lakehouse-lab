# Demos

This document provides step-by-step instructions for running demos with SQL, PyFlink, and Java.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Demo 1: Flink SQL - Kafka to Iceberg](#demo-1-flink-sql---kafka-to-iceberg)
- [Demo 2: PyFlink - EWMA Anomaly Detection](#demo-2-pyflink---ewma-anomaly-detection)
- [Demo 3: PyFlink - CEP Pattern Detection](#demo-3-pyflink---cep-pattern-detection)
- [Demo 4: Java - Stateful Deduplication](#demo-4-java---stateful-deduplication)

---

## Prerequisites

### 1. Start the Infrastructure

```bash
# Start all services
make up

# Wait for services to be ready (~30 seconds)
make health

# Initialize topics, buckets, and catalog
make seed
```

### 2. Verify Services

```bash
# Check service health
docker ps

# Should see: kafka, postgres, minio, iceberg-rest, flink-jobmanager, flink-taskmanager
```

### 3. Access Web UIs

- **Flink JobManager:** http://localhost:8081
- **MinIO Console:** http://localhost:9001 (admin/password123)
- **Flink SQL Gateway:** http://localhost:8083/v1/info

---

## Demo 1: Flink SQL - Kafka to Iceberg

### Step 1: Open Flink SQL Client

```bash
docker exec -it flink-jobmanager ./bin/sql-client.sh
```

### Step 2: Create Iceberg Catalog

```sql
CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181',
    'warehouse' = 's3a://lakehouse/warehouse',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://minio:9000',
    's3.path-style-access' = 'true',
    's3.access-key-id' = 'admin',
    's3.secret-access-key' = 'password123'
);

USE CATALOG iceberg_catalog;
CREATE DATABASE IF NOT EXISTS streaming_lakehouse;
USE streaming_lakehouse;
```

### Step 3: Create Kafka Source Table

```sql
CREATE TABLE kafka_sensors (
    device_id STRING,
    ts BIGINT,
    x DOUBLE,
    event_time AS TO_TIMESTAMP_LTZ(ts, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'sensors',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-sql-demo',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);
```

### Step 4: Create Iceberg Sink Table

```sql
CREATE TABLE sensors_iceberg (
    device_id STRING,
    ts BIGINT,
    x DOUBLE,
    event_time TIMESTAMP(3),
    ingestion_time TIMESTAMP(3),
    date_partition STRING,
    PRIMARY KEY (device_id, ts) NOT ENFORCED
) PARTITIONED BY (date_partition) WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);
```

### Step 5: Produce Test Data

Open a new terminal:

```bash
docker exec -it kafka kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic sensors
```

Paste these events:

```json
{"device_id": "sensor_001", "ts": 1705161600000, "x": 23.5}
{"device_id": "sensor_002", "ts": 1705161601000, "x": 24.1}
{"device_id": "sensor_001", "ts": 1705161602000, "x": 23.8}
```

### Step 6: Stream Data to Iceberg

```sql
INSERT INTO sensors_iceberg
SELECT
    device_id,
    ts,
    x,
    event_time,
    CURRENT_TIMESTAMP AS ingestion_time,
    DATE_FORMAT(event_time, 'yyyy-MM-dd') AS date_partition
FROM kafka_sensors;
```

### Step 7: Query the Iceberg Table

Open new SQL client:

```bash
docker exec -it flink-jobmanager ./bin/sql-client.sh
```

```sql
USE CATALOG iceberg_catalog;
USE streaming_lakehouse;
SELECT * FROM sensors_iceberg;
```

---

## Demo 2: PyFlink - EWMA Anomaly Detection

### Step 1: Produce Normal Sensor Data

```bash
docker exec -it kafka kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic sensors
```

```json
{"device_id": "temp_sensor_01", "ts": 1705161600000, "x": 25.1}
{"device_id": "temp_sensor_01", "ts": 1705161601000, "x": 25.3}
{"device_id": "temp_sensor_01", "ts": 1705161602000, "x": 24.9}
{"device_id": "temp_sensor_01", "ts": 1705161603000, "x": 25.0}
```

### Step 2: Start EWMA Job

```bash
make py job=ewma_ad
```

### Step 3: Consume Alerts

```bash
docker exec -it kafka kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic sensors-alerts \
    --from-beginning
```

### Step 4: Inject Anomalies

Back in producer terminal:

```json
{"device_id": "temp_sensor_01", "ts": 1705161605000, "x": 45.0}
{"device_id": "temp_sensor_01", "ts": 1705161607000, "x": 5.0}
```

Observe anomaly alerts with `is_anomaly: true`.

---

## Demo 3: PyFlink - CEP Pattern Detection

### Step 1: Start CEP Job

```bash
make py job=sensor_cep
```

### Step 2: Consume Sessions

```bash
docker exec -it kafka kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic door_sessions \
    --from-beginning
```

### Step 3: Produce Door Events

```bash
docker exec -it kafka kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic sensors
```

```json
{"device_id": "door_001", "ts": 1705161600000, "event_state": "OPEN"}
{"device_id": "door_001", "ts": 1705161630000, "event_state": "CLOSE"}
```

Expected session output:

```json
{
  "device_id": "door_001",
  "open_ts": 1705161600000,
  "close_ts": 1705161630000,
  "duration_ms": 30000
}
```

---

## Demo 4: Java - Stateful Deduplication

### Step 1: Build Java Job

```bash
./gradlew :flink-java-jobs:build
```

### Step 2: Produce Duplicate Events

```bash
docker exec -it kafka kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic events-raw
```

```json
{"event_id": "evt-001", "timestamp": 1705161600000, "data": "first"}
{"event_id": "evt-002", "timestamp": 1705161601000, "data": "second"}
{"event_id": "evt-001", "timestamp": 1705161602000, "data": "duplicate"}
{"event_id": "evt-003", "timestamp": 1705161603000, "data": "third"}
```

### Step 3: Start Dedup Job

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
./gradlew :flink-java-jobs:runDedupJob
```

### Step 4: Consume Deduplicated Events

```bash
docker exec -it kafka kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic events-deduped \
    --from-beginning
```

Expected: Only unique events (evt-001, evt-002, evt-003).

---

## Tips

1. **Monitor jobs**: http://localhost:8081
2. **Check logs**: `docker logs flink-jobmanager -f`
3. **List topics**: `docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list`

---

## References

- [Flink SQL](https://flink.apache.org/docs)
- [PyFlink](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/)
- [Iceberg](https://iceberg.apache.org/docs/)

# Troubleshooting Guide

This guide covers common issues, required environment variables, and solutions for the Streaming Lakehouse Lab.

## Table of Contents

- [Required Environment Variables](#required-environment-variables)
- [Service Connection Issues](#service-connection-issues)
- [Connector Issues](#connector-issues)
- [Performance Issues](#performance-issues)
- [Data Quality Issues](#data-quality-issues)

---

## Required Environment Variables

### Core Infrastructure

All environment variables below are defined in `.env.example`. Copy to `.env` for local overrides:

```bash
cp .env.example .env
```

### Kafka Configuration

```bash
# Kafka broker connection
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_BROKER=kafka:9092

# KRaft cluster configuration
KAFKA_CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk  # Sample cluster ID
KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093

# Topic names
KAFKA_TOPIC_EVENTS=streaming.events
KAFKA_TOPIC_METRICS=streaming.metrics
KAFKA_TOPIC_ORDERS=streaming.orders
```

**Required for**: All Kafka connectors, data generation scripts

### PostgreSQL Configuration

```bash
# PostgreSQL connection
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=iceberg_catalog
POSTGRES_USER=iceberg
POSTGRES_PASSWORD=iceberg123  # SAMPLE - Change in production

# JDBC URL (auto-constructed)
CATALOG_JDBC_URL=jdbc:postgresql://postgres:5432/iceberg_catalog
CATALOG_JDBC_USER=iceberg
CATALOG_JDBC_PASSWORD=iceberg123  # SAMPLE - Change in production
```

**Required for**: Iceberg catalog, PostgreSQL CDC connector, database operations

### MinIO (S3) Configuration

```bash
# MinIO server
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password123  # SAMPLE - Change in production
MINIO_ENDPOINT=http://minio:9000
MINIO_CONSOLE_PORT=9001
MINIO_API_PORT=9000

# S3 bucket settings
S3_BUCKET=lakehouse
S3_WAREHOUSE_PATH=s3a://lakehouse/warehouse
S3_ACCESS_KEY=admin
S3_SECRET_KEY=password123  # SAMPLE - Change in production

# AWS SDK configuration (for MinIO compatibility)
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=admin
AWS_SECRET_ACCESS_KEY=password123  # SAMPLE - Change in production
AWS_S3_ENDPOINT=http://minio:9000
AWS_S3_PATH_STYLE_ACCESS=true  # REQUIRED for MinIO
```

**Required for**: Iceberg table storage, S3 file operations

### Flink Configuration

```bash
# Flink cluster
FLINK_JOBMANAGER_RPC_ADDRESS=flink-jobmanager
FLINK_JOBMANAGER_RPC_PORT=6123
FLINK_JOBMANAGER_UI_PORT=8081
FLINK_TASKMANAGER_SLOTS=4

# Flink SQL Gateway
FLINK_SQL_GATEWAY_PORT=8083

# Resource allocation
FLINK_JOBMANAGER_MEMORY=1600m
FLINK_TASKMANAGER_MEMORY=1728m
FLINK_TASKMANAGER_CPU=1.0
```

**Required for**: All Flink jobs, SQL Gateway operations

### Iceberg Configuration

```bash
# Iceberg catalog settings
ICEBERG_CATALOG_NAME=iceberg_catalog
ICEBERG_CATALOG_TYPE=jdbc
ICEBERG_WAREHOUSE=s3a://lakehouse/warehouse
ICEBERG_NAMESPACE=streaming_lakehouse

# Table settings
ICEBERG_FILE_FORMAT=parquet
ICEBERG_COMPRESSION=snappy
```

**Required for**: Iceberg catalog creation, table operations

### Network Configuration

```bash
# Docker networking
COMPOSE_PROJECT_NAME=lakehouse
DOCKER_NETWORK=lakehouse-network
```

**Required for**: All services to communicate

---

## Service Connection Issues

### Services Not Starting

**Symptom**: `make up` fails or services crash immediately

**Solutions**:

1. Check Docker resources:
   ```bash
   docker system df
   docker system prune -af  # Warning: removes all unused resources
   ```

2. Check service logs:
   ```bash
   make logs SERVICE=kafka
   make logs SERVICE=flink-jobmanager
   ```

3. Verify port conflicts:
   ```bash
   # Check if ports are already in use
   netstat -an | grep -E ':(5432|8081|8083|9000|9001|9092)'
   ```

4. Restart services:
   ```bash
   make down
   make up
   ```

### Kafka Not Ready

**Symptom**: "Connection refused" when connecting to Kafka

**Solutions**:

1. Wait for Kafka to initialize (KRaft mode takes ~30 seconds):
   ```bash
   docker exec kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092
   ```

2. Check Kafka logs:
   ```bash
   make logs SERVICE=kafka
   ```

3. Verify topics were created:
   ```bash
   docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
   ```

4. Recreate topics:
   ```bash
   make topics
   ```

### PostgreSQL Connection Failed

**Symptom**: "Connection refused" or "authentication failed"

**Solutions**:

1. Verify PostgreSQL is running:
   ```bash
   docker exec postgres pg_isready -U iceberg
   ```

2. Check credentials match `.env`:
   ```bash
   docker exec postgres psql -U iceberg -d iceberg_catalog -c '\dt'
   ```

3. Verify init script ran:
   ```bash
   make postgres-shell
   # In psql:
   SELECT * FROM users;
   ```

### MinIO Not Accessible

**Symptom**: "Access Denied" or connection timeout

**Solutions**:

1. Check MinIO is running:
   ```bash
   docker exec minio mc admin info local
   ```

2. Verify credentials:
   ```bash
   # Console: http://localhost:9001
   # Username: admin
   # Password: password123
   ```

3. Check buckets exist:
   ```bash
   make minio-ls
   # Or manually:
   docker exec minio mc ls local/lakehouse/
   ```

4. Recreate buckets:
   ```bash
   make buckets
   ```

### Iceberg REST Catalog Unavailable

**Symptom**: "Failed to connect to catalog" or HTTP 404

**Solutions**:

1. Verify REST catalog is running:
   ```bash
   curl http://localhost:8181/v1/config
   ```

2. Check dependencies (PostgreSQL, MinIO):
   ```bash
   make health
   ```

3. Review catalog logs:
   ```bash
   make logs SERVICE=iceberg-rest
   ```

4. Reinitialize catalog:
   ```bash
   make catalog
   ```

---

## Connector Issues

### Iceberg Catalog - "Path style access" Error

**Symptom**: `S3Exception: The authorization header is malformed`

**Solution**: Ensure `s3.path-style-access = 'true'` is set in catalog config:

```sql
-- In connectors/catalog-iceberg.sql
's3.path-style-access' = 'true',  -- Required for MinIO
```

### Iceberg Catalog - "Warehouse not found"

**Symptom**: `NoSuchBucketException` or `FileNotFoundException`

**Solutions**:

1. Create warehouse bucket:
   ```bash
   make buckets
   ```

2. Verify bucket path:
   ```bash
   docker exec minio mc ls local/lakehouse/warehouse/
   ```

3. Check S3 endpoint configuration:
   ```sql
   's3.endpoint' = 'http://minio:9000',  -- Must use service name inside Docker
   ```

### PostgreSQL CDC - Replication Slot Error

**Symptom**: `ERROR: replication slot "flink_cdc_slot" already exists`

**Solutions**:

1. Drop existing slot:
   ```bash
   make postgres-shell
   # In psql:
   SELECT pg_drop_replication_slot('flink_cdc_slot');
   ```

2. Use unique slot name per job:
   ```sql
   'slot.name' = 'flink_cdc_slot_unique_name',
   ```

### PostgreSQL CDC - "Logical replication not enabled"

**Symptom**: `ERROR: logical decoding requires wal_level >= logical`

**Solution**: This stack uses PostgreSQL Alpine which may need configuration. Add to `docker/compose.yml`:

```yaml
postgres:
  command:
    - "postgres"
    - "-c"
    - "wal_level=logical"
    - "-c"
    - "max_replication_slots=5"
```

### Kafka Connector - "Topic does not exist"

**Symptom**: `KafkaException: Topic 'topic_name' does not exist`

**Solutions**:

1. Create topics:
   ```bash
   make topics
   ```

2. Create custom topic:
   ```bash
   docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 \
     --create --topic your_topic --partitions 3 --replication-factor 1
   ```

3. Enable auto-create (not recommended for production):
   ```sql
   'properties.allow.auto.create.topics' = 'true'
   ```

### Kafka Connector - JSON Parse Errors

**Symptom**: `JsonParseException` or malformed data

**Solutions**:

1. Enable error tolerance:
   ```sql
   'json.ignore-parse-errors' = 'true',
   'json.fail-on-missing-field' = 'false'
   ```

2. Validate JSON format:
   ```bash
   docker exec kafka kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 \
     --topic your_topic --from-beginning --max-messages 1
   ```

3. Check timestamp format:
   ```sql
   'json.timestamp-format.standard' = 'ISO-8601'  -- Use ISO format
   ```

### Kafka Connector - Consumer Group Lag

**Symptom**: Flink job not processing new messages

**Solutions**:

1. Check consumer group status:
   ```bash
   docker exec kafka kafka-consumer-groups.sh \
     --bootstrap-server localhost:9092 --group flink-consumer-group --describe
   ```

2. Reset offsets:
   ```bash
   docker exec kafka kafka-consumer-groups.sh \
     --bootstrap-server localhost:9092 --group flink-consumer-group \
     --reset-offsets --to-earliest --topic your_topic --execute
   ```

3. Change startup mode:
   ```sql
   'scan.startup.mode' = 'latest-offset'  -- or 'earliest-offset'
   ```

---

## Performance Issues

### Flink Job Slow or Backpressure

**Symptom**: High backpressure, slow processing

**Solutions**:

1. Increase parallelism:
   ```sql
   SET 'parallelism.default' = '4';
   ```

2. Increase TaskManager resources in `docker/compose.yml`:
   ```yaml
   FLINK_TASKMANAGER_MEMORY: 2048m
   ```

3. Tune checkpointing:
   ```yaml
   # infra/flink/conf/flink-conf.yaml
   execution.checkpointing.interval: 120s  # Increase interval
   ```

4. Add more TaskManagers:
   ```bash
   docker compose -f docker/compose.yml up -d --scale flink-taskmanager=3
   ```

### MinIO Slow Performance

**Symptom**: Slow writes to Iceberg tables

**Solutions**:

1. Check MinIO resource usage:
   ```bash
   docker stats minio
   ```

2. Increase MinIO memory in `docker/compose.yml`

3. Use compression:
   ```sql
   'write.parquet.compression-codec' = 'snappy'
   ```

### Kafka Lag Increasing

**Symptom**: Consumer lag grows continuously

**Solutions**:

1. Increase Kafka partitions:
   ```bash
   docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 \
     --alter --topic your_topic --partitions 6
   ```

2. Increase Flink parallelism to match partitions

3. Optimize consumer config:
   ```sql
   'properties.fetch.min.bytes' = '1048576',  -- 1MB
   'properties.fetch.max.wait.ms' = '500'
   ```

---

## Data Quality Issues

### Duplicate Records in Iceberg Tables

**Symptom**: Same event appears multiple times

**Solutions**:

1. Check for duplicate sources:
   ```sql
   SELECT event_id, COUNT(*)
   FROM events_iceberg
   GROUP BY event_id
   HAVING COUNT(*) > 1;
   ```

2. Use UPSERT mode for changelog streams:
   ```sql
   PRIMARY KEY (event_id) NOT ENFORCED
   ```

3. Enable exactly-once semantics:
   ```yaml
   # infra/flink/conf/flink-conf.yaml
   execution.checkpointing.mode: EXACTLY_ONCE
   ```

### Missing Data / Data Loss

**Symptom**: Expected data not in tables

**Solutions**:

1. Check Flink job status:
   ```bash
   make list-jobs
   ```

2. Verify Kafka topics have data:
   ```bash
   docker exec kafka kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 --topic your_topic --from-beginning
   ```

3. Check watermarks and late data:
   ```sql
   WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND  -- Increase tolerance
   ```

### Schema Evolution Issues

**Symptom**: "Schema mismatch" after adding columns

**Solutions**:

1. Iceberg supports schema evolution:
   ```sql
   ALTER TABLE events_iceberg ADD COLUMN new_field STRING;
   ```

2. For Kafka, ensure JSON has default handling:
   ```sql
   'json.fail-on-missing-field' = 'false'
   ```

---

## Quick Health Check

Run this to verify all services are healthy:

```bash
make health
```

Expected output:
```
Kafka: ✓
Flink JobManager: ✓
Flink SQL Gateway: ✓
MinIO: ✓
PostgreSQL: ✓
```

---

## Getting Help

1. Check service logs:
   ```bash
   make logs SERVICE=service_name
   ```

2. Verify environment variables:
   ```bash
   docker compose -f docker/compose.yml config
   ```

3. Run smoke tests:
   ```bash
   make test-smoke
   ```

4. Review configuration files:
   - `docker/compose.yml` - Service definitions
   - `infra/flink/conf/flink-conf.yaml` - Flink configuration
   - `.env.example` - Environment variable reference
   - `connectors/*.sql` - Connector templates

---

## Common Error Messages

| Error | Likely Cause | Solution |
|-------|--------------|----------|
| `Connection refused` | Service not started | `make up`, wait for healthchecks |
| `Access Denied (S3)` | Wrong credentials or path-style-access | Check `.env`, verify `s3.path-style-access='true'` |
| `Topic does not exist` | Kafka topics not created | `make topics` |
| `Catalog not found` | Iceberg REST not initialized | `make catalog` |
| `Replication slot already exists` | Previous CDC job didn't clean up | Drop slot in psql |
| `JsonParseException` | Malformed JSON in Kafka | Enable `json.ignore-parse-errors='true'` |
| `CheckpointException` | Checkpoint storage issue | Check MinIO/S3 connectivity |
| `NoClassDefFoundError` | Missing connector JAR | Add JAR to Flink lib directory |

---

## Environment Variable Validation

To validate all required variables are set:

```bash
# Check if .env exists
if [ -f .env ]; then
  echo "✓ .env file found"
else
  echo "✗ .env file not found. Copy from .env.example"
fi

# Validate key variables
for var in POSTGRES_HOST KAFKA_BOOTSTRAP_SERVERS MINIO_ROOT_USER S3_BUCKET; do
  if [ -z "${!var}" ]; then
    echo "✗ $var not set"
  else
    echo "✓ $var = ${!var}"
  fi
done
```

For more help, see:
- Flink Documentation: https://flink.apache.org/docs/stable/
- Iceberg Documentation: https://iceberg.apache.org/docs/latest/
- Kafka Documentation: https://kafka.apache.org/documentation/

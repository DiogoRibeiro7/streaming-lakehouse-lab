# Flink Java Jobs - Stateful Deduplication

This module contains Flink streaming jobs implemented in Java 21 with Flink 2.1.x.

## StatefulDedupJob

A production-ready event deduplication job using Flink's keyed ValueState with automatic TTL-based cleanup.

### Features

- **Stateful Deduplication**: Uses Flink's ValueState to track seen event IDs
- **Automatic TTL Cleanup**: State expires after 10 minutes (configurable) to prevent unbounded growth
- **Event-Time Processing**: Watermarks for handling out-of-order events
- **Exactly-Once Semantics**: Checkpointing enabled for fault tolerance
- **Kafka Integration**: Reads from and writes to Kafka topics

### Architecture

```
[Kafka: events-raw] → [Parse JSON] → [Key by event_id] → [Dedup Filter] → [Kafka: events-deduped]
                                           ↓
                                   [ValueState + TTL]
```

## Connecting to Kafka Topics

### Input Topic Configuration

The job reads from the **events-raw** Kafka topic (configurable via `KAFKA_INPUT_TOPIC`).

#### Input Event Schema

Events must be JSON with at minimum an `event_id` field:

```json
{
  "event_id": "unique-event-123",
  "timestamp": 1699900000000,
  "data": {
    "key": "value"
  }
}
```

**Required Fields:**
- `event_id` (String): Unique identifier for deduplication key
- `timestamp` (Long): Event timestamp in milliseconds (epoch) for watermarks

### Output Topic Configuration

Deduplicated events are written to **events-deduped** Kafka topic (configurable via `KAFKA_OUTPUT_TOPIC`).

The output format is identical to the input - only duplicate events are filtered out.

### Kafka Connection Settings

#### Environment Variables

Configure Kafka connection using these environment variables:

```bash
# Kafka broker addresses (comma-separated for multiple brokers)
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Input topic for raw events
export KAFKA_INPUT_TOPIC=events-raw

# Output topic for deduplicated events
export KAFKA_OUTPUT_TOPIC=events-deduped

# Consumer group ID
export KAFKA_GROUP_ID=dedup-job-group

# State TTL in minutes (how long to remember event IDs)
export STATE_TTL_MINUTES=10

# Flink job parallelism
export FLINK_PARALLELISM=2

# Checkpoint interval in milliseconds
export FLINK_CHECKPOINT_INTERVAL=60000
```

#### Example: Connecting to Docker Kafka

If running Kafka in Docker (via docker-compose):

```bash
# From host machine
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# From within Docker network
export KAFKA_BOOTSTRAP_SERVERS=kafka:9092
```

#### Example: Connecting to Confluent Cloud

```bash
export KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092

# Note: Additional SASL/SSL configuration would be needed in the code
# for production Confluent Cloud connections
```

### Creating Topics

Before running the job, ensure Kafka topics exist:

```bash
# Create input topic
kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic events-raw \
  --partitions 3 \
  --replication-factor 1

# Create output topic
kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic events-deduped \
  --partitions 3 \
  --replication-factor 1
```

Or using the Makefile at the project root:

```bash
make topics
```

### Testing the Job

#### 1. Build the Job

```bash
# From project root
./gradlew :flink-java-jobs:build

# Build shadow JAR for cluster submission
./gradlew :flink-java-jobs:shadowJar
```

#### 2. Run Locally

```bash
# Using Gradle
./gradlew :flink-java-jobs:run

# Or using the custom task
./gradlew :flink-java-jobs:runDedupJob
```

#### 3. Produce Test Events

Open a new terminal and produce some test events:

```bash
# Start Kafka console producer
kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic events-raw

# Paste these test events (one per line):
{"event_id": "evt-001", "timestamp": 1699900000000, "data": "first"}
{"event_id": "evt-002", "timestamp": 1699900001000, "data": "second"}
{"event_id": "evt-001", "timestamp": 1699900002000, "data": "duplicate"}
{"event_id": "evt-003", "timestamp": 1699900003000, "data": "third"}
```

#### 4. Consume Deduplicated Output

Open another terminal to consume deduplicated events:

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic events-deduped \
  --from-beginning

# Expected output (evt-001 duplicate filtered out):
{"event_id": "evt-001", "timestamp": 1699900000000, "data": "first"}
{"event_id": "evt-002", "timestamp": 1699900001000, "data": "second"}
{"event_id": "evt-003", "timestamp": 1699900003000, "data": "third"}
```

### Submitting to Flink Cluster

#### 1. Build Shadow JAR

```bash
./gradlew :flink-java-jobs:shadowJar
```

The JAR will be created at:
```
flink-java-jobs/build/libs/flink-dedup-job-0.1.0.jar
```

#### 2. Submit to Flink

```bash
# Using Flink CLI
flink run \
  --class com.lakehouse.dedup.StatefulDedupJob \
  flink-java-jobs/build/libs/flink-dedup-job-0.1.0.jar

# With parallelism override
flink run \
  --parallelism 4 \
  --class com.lakehouse.dedup.StatefulDedupJob \
  flink-java-jobs/build/libs/flink-dedup-job-0.1.0.jar
```

#### 3. Monitor via Flink Web UI

Access the Flink JobManager UI to monitor the job:

```
http://localhost:8081
```

### How Deduplication Works

1. **Keying**: Events are keyed by `event_id` field
2. **State Check**: For each event, the job checks if the ID exists in ValueState
3. **First Occurrence**: If state is null/expired, marks as seen and passes through
4. **Duplicate Detection**: If state exists, filters out the duplicate
5. **TTL Cleanup**: State automatically expires after 10 minutes (configurable)

### State TTL Behavior

The job uses **10-minute TTL** by default:

- **Within 10 minutes**: Duplicate event IDs are filtered out
- **After 10 minutes**: State expires, same event ID can be processed again
- **Cleanup**: Background incremental cleanup prevents state bloat

This sliding window approach is ideal for:
- Near-real-time deduplication
- Preventing memory issues with infinite state
- Handling late arrivals within a bounded window

### Performance Tuning

#### Parallelism

Adjust based on throughput requirements:

```bash
export FLINK_PARALLELISM=8  # Higher parallelism for more throughput
```

#### State TTL

Balance between dedup accuracy and state size:

```bash
export STATE_TTL_MINUTES=5   # Shorter TTL = less state, faster cleanup
export STATE_TTL_MINUTES=30  # Longer TTL = better dedup, more state
```

#### Checkpointing

Tune checkpoint interval for recovery time vs. overhead:

```bash
export FLINK_CHECKPOINT_INTERVAL=30000   # More frequent (30s)
export FLINK_CHECKPOINT_INTERVAL=300000  # Less frequent (5min)
```

## Project Structure

```
flink-java-jobs/
├── build.gradle.kts                       # Gradle build configuration
├── README.md                              # This file
└── src/
    └── main/
        └── java/
            └── com/
                └── lakehouse/
                    └── dedup/
                        └── StatefulDedupJob.java   # Main job implementation
```

## Building and Testing

```bash
# Clean and build
./gradlew :flink-java-jobs:clean build

# Run tests
./gradlew :flink-java-jobs:test

# Generate shadow JAR
./gradlew :flink-java-jobs:shadowJar

# Run locally
./gradlew :flink-java-jobs:runDedupJob
```

## Requirements

- **Java**: 21 or higher
- **Flink**: 2.1.x
- **Kafka**: Any version compatible with kafka-clients 4.1.0
- **Gradle**: 8.x (wrapper included)

## License

MIT License - See project root LICENSE file.

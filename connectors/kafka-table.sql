-- ============================================================
-- Kafka Source/Sink Table Template (JSON)
-- ============================================================
-- Generic Kafka connector template for creating source and sink
-- tables with JSON value format. Copy and customize for your use case.
--
-- Usage:
--   \i connectors/kafka-table.sql
--   -- Then modify the table definition below
--
-- Environment Variables (from .env):
--   KAFKA_BOOTSTRAP_SERVERS=kafka:9092
--
-- Common Topics (created by make topics):
--   - ticks
--   - sensors
--   - cdc.app.public_users
--
-- See: docs/TROUBLESHOOTING.md for Kafka connector issues
-- ============================================================

-- ============================================================
-- Example 1: Kafka Source Table (Read from Kafka)
-- ============================================================

CREATE TABLE IF NOT EXISTS kafka_source_example (
  -- Define your schema here
  event_id STRING,
  event_type STRING,
  user_id STRING,
  event_time TIMESTAMP(3),
  payload MAP<STRING, STRING>,

  -- Event time watermark for windowing
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND,

  -- Optional: Primary key for changelog streams
  PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',

  -- Kafka connection
  'topic' = 'your_topic_name',  -- CHANGE THIS
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-consumer-group',

  -- Read behavior
  'scan.startup.mode' = 'earliest-offset',  -- Options: earliest-offset, latest-offset, group-offsets, timestamp
  -- 'scan.startup.timestamp-millis' = '1672531200000',  -- Unix timestamp for 'timestamp' mode

  -- JSON format configuration
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.ignore-parse-errors' = 'false',
  'json.fail-on-missing-field' = 'false'
);

-- ============================================================
-- Example 2: Kafka Sink Table (Write to Kafka)
-- ============================================================

CREATE TABLE IF NOT EXISTS kafka_sink_example (
  -- Define your schema here
  event_id STRING,
  event_type STRING,
  user_id STRING,
  event_time TIMESTAMP(3),
  payload MAP<STRING, STRING>,
  PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',

  -- Kafka connection
  'topic' = 'your_output_topic',  -- CHANGE THIS
  'properties.bootstrap.servers' = 'kafka:9092',

  -- Write behavior
  'sink.partitioner' = 'default',  -- Options: default, fixed, round-robin

  -- JSON format configuration
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.encode.decimal-as-plain-number' = 'false'
);

-- ============================================================
-- Example 3: Kafka Source with AVRO (commented out)
-- ============================================================

/*
CREATE TABLE kafka_avro_source (
  event_id STRING,
  event_type STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'avro_topic',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',

  -- AVRO format with Schema Registry
  'format' = 'avro',
  'avro.schema-registry.url' = 'http://schema-registry:8081',
  'avro.schema-registry.subject' = 'avro_topic-value'
);
*/

-- ============================================================
-- Example 4: Kafka Upsert (Changelog) Table
-- ============================================================

CREATE TABLE IF NOT EXISTS kafka_upsert_example (
  -- Primary key required for upsert mode
  user_id STRING,
  username STRING,
  email STRING,
  last_login TIMESTAMP(3),
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',

  -- Kafka connection
  'topic' = 'user_profiles',
  'properties.bootstrap.servers' = 'kafka:9092',

  -- Key format (typically simple)
  'key.format' = 'json',

  -- Value format
  'value.format' = 'json',
  'value.json.timestamp-format.standard' = 'ISO-8601'
);

-- ============================================================
-- Example 5: Sensors Topic Template (for make topics)
-- ============================================================

CREATE TABLE IF NOT EXISTS sensors_kafka (
  sensor_id STRING,
  sensor_type STRING,
  temperature DOUBLE,
  humidity DOUBLE,
  timestamp_ms BIGINT,
  event_time AS TO_TIMESTAMP(FROM_UNIXTIME(timestamp_ms / 1000)),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensors',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-sensors-consumer',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);

-- ============================================================
-- Example 6: Ticks Topic Template (for make topics)
-- ============================================================

CREATE TABLE IF NOT EXISTS ticks_kafka (
  symbol STRING,
  price DOUBLE,
  volume BIGINT,
  timestamp_ms BIGINT,
  event_time AS TO_TIMESTAMP(FROM_UNIXTIME(timestamp_ms / 1000)),
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'ticks',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-ticks-consumer',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);

-- ============================================================
-- Usage Examples
-- ============================================================

-- Read from Kafka source
-- SELECT * FROM kafka_source_example;

-- Stream data from source to sink
-- INSERT INTO kafka_sink_example
-- SELECT * FROM kafka_source_example;

-- Filter and transform
-- INSERT INTO kafka_sink_example
-- SELECT event_id, event_type, user_id, event_time, payload
-- FROM kafka_source_example
-- WHERE event_type = 'click';

-- Windowed aggregation
-- INSERT INTO kafka_sink_example
-- SELECT
--   TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
--   event_type,
--   COUNT(*) AS event_count
-- FROM kafka_source_example
-- GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), event_type;

-- ============================================================
-- PostgreSQL CDC Table Configuration
-- ============================================================
-- Creates a Flink CDC source table that streams changes from
-- PostgreSQL's public.users table using Debezium connector.
--
-- Usage:
--   \i connectors/postgres-cdc.sql
--
-- Prerequisites:
--   1. PostgreSQL with logical replication enabled
--   2. Flink CDC connector JAR in classpath
--   3. Network access to PostgreSQL
--
-- Environment Variables (from .env):
--   POSTGRES_HOST=postgres
--   POSTGRES_PORT=5432
--   POSTGRES_DB=iceberg_catalog
--   POSTGRES_USER=iceberg
--   POSTGRES_PASSWORD=iceberg123
--
-- Kafka Topic Output:
--   cdc.app.public_users (created by make topics)
--
-- See: docs/TROUBLESHOOTING.md for CDC troubleshooting
-- ============================================================

-- Create CDC source table for PostgreSQL users
CREATE TABLE IF NOT EXISTS users_cdc (
  -- Table columns matching public.users schema
  id INT,
  username STRING,
  email STRING,
  created_at TIMESTAMP(3),

  -- CDC metadata columns
  op_type STRING METADATA FROM 'op_type' VIRTUAL,  -- INSERT, UPDATE, DELETE
  database_name STRING METADATA FROM 'database_name' VIRTUAL,
  schema_name STRING METADATA FROM 'schema_name' VIRTUAL,
  table_name STRING METADATA FROM 'table_name' VIRTUAL,

  -- Watermark for event time processing
  WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND,

  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  -- Connector type
  'connector' = 'postgres-cdc',

  -- PostgreSQL connection
  'hostname' = 'postgres',
  'port' = '5432',
  'username' = 'iceberg',
  'password' = 'iceberg123',
  'database-name' = 'iceberg_catalog',
  'schema-name' = 'public',
  'table-name' = 'users',

  -- CDC behavior
  'slot.name' = 'flink_cdc_slot',
  'decoding.plugin.name' = 'pgoutput',
  'debezium.snapshot.mode' = 'initial',  -- Options: initial, never, always

  -- Performance tuning
  'scan.incremental.snapshot.enabled' = 'true',
  'scan.incremental.snapshot.chunk.size' = '8192'
);

-- Example: Mirror CDC changes to Kafka topic
/*
CREATE TABLE IF NOT EXISTS users_cdc_kafka (
  id INT,
  username STRING,
  email STRING,
  created_at TIMESTAMP(3),
  op_type STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'cdc.app.public_users',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.ignore-parse-errors' = 'false'
);

-- Stream CDC changes to Kafka
INSERT INTO users_cdc_kafka
SELECT id, username, email, created_at, op_type
FROM users_cdc;
*/

-- Example: Query current state
-- SELECT * FROM users_cdc;

-- Example: Track only inserts
-- SELECT * FROM users_cdc WHERE op_type = 'INSERT';

-- Example: Count changes by operation type
-- SELECT op_type, COUNT(*) as change_count
-- FROM users_cdc
-- GROUP BY op_type;

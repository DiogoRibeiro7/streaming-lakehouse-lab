-- ============================================================
-- PostgreSQL CDC to Iceberg Pipeline
-- ============================================================
-- This script creates a streaming data pipeline that:
--   1. Captures changes from PostgreSQL users table via CDC
--   2. Writes them to an Iceberg table in the lakehouse
--
-- Database: lakehouse.app (iceberg_catalog.app in Flink notation)
-- Table: users (bucketed by id for uniform distribution)
--
-- Usage:
--   make sql file=flink-sql/01_cdc_pg_to_iceberg.sql
--
-- Expected outcome:
--   - Iceberg table created with 2 seeded users (alice, bob)
--   - Streaming job continuously syncs CDC changes
-- ============================================================

-- Step 1: Set up CDC source table in default catalog
-- This creates the users_cdc table that reads from PostgreSQL
USE CATALOG default_catalog;

-- Include PostgreSQL CDC connector definition
\i connectors/postgres-cdc.sql

-- Step 2: Set up Iceberg catalog and switch to it
-- This creates and activates the iceberg_catalog
\i connectors/catalog-iceberg.sql

-- At this point, we're in iceberg_catalog.streaming_lakehouse
-- Now create the 'app' database for application data

-- Step 3: Create 'app' database in Iceberg catalog
CREATE DATABASE IF NOT EXISTS app
COMMENT 'Application data namespace';

USE app;

-- Step 4: Create Iceberg users table with bucketing partition
-- Bucketing by id provides uniform distribution across 16 buckets
CREATE TABLE IF NOT EXISTS users (
  id INT,
  username STRING,
  email STRING,
  created_at TIMESTAMP(3),
  PRIMARY KEY (id) NOT ENFORCED
) PARTITIONED BY (bucket(16, id))
WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'snappy',
  'write.metadata.compression-codec' = 'gzip'
);

-- Verify table was created
SHOW TABLES;

-- Step 5: Stream CDC changes to Iceberg table
-- This INSERT starts a continuous streaming Flink job that:
--   - Performs initial snapshot of existing data (alice, bob)
--   - Continuously streams any new changes (INSERT/UPDATE/DELETE)
--   - Writes to Iceberg with exactly-once semantics

INSERT INTO users
SELECT
  id,
  username,
  email,
  created_at
FROM default_catalog.default_database.users_cdc;

-- ============================================================
-- Notes:
-- ============================================================
-- - This job runs continuously; stop with: make cancel-job JOB_ID=xxx
-- - Initial snapshot will contain seeded users from infra/postgres/init.sql
-- - Partition strategy: bucket(16, id) distributes data evenly
-- - CDC captures INSERT, UPDATE, DELETE operations
-- - Iceberg provides ACID guarantees and time travel capabilities
--
-- To verify data after job starts:
--   make flink-shell
--   USE CATALOG iceberg_catalog;
--   USE app;
--   SELECT * FROM users;
-- ============================================================

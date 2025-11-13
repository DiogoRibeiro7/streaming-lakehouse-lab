-- ============================================================
-- Iceberg Catalog Setup SQL
-- ============================================================
-- This script creates and configures the Iceberg catalog in Flink
-- for use with the Flink SQL Gateway or Flink SQL Client
--
-- Usage:
--   Execute via Flink SQL Gateway REST API or SQL Client
-- ============================================================

-- Create Iceberg catalog
CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
  'type' = 'iceberg',
  'catalog-type' = 'jdbc',
  'uri' = 'jdbc:postgresql://postgres:5432/iceberg_catalog',
  'jdbc.user' = 'iceberg',
  'jdbc.password' = 'iceberg123',
  'warehouse' = 's3a://lakehouse/warehouse',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://minio:9000',
  's3.path-style-access' = 'true',
  's3.access-key-id' = 'admin',
  's3.secret-access-key' = 'password123'
);

-- Use the catalog
USE CATALOG iceberg_catalog;

-- Create database/namespace
CREATE DATABASE IF NOT EXISTS streaming_lakehouse
COMMENT 'Streaming lakehouse namespace for event data';

-- Use the database
USE streaming_lakehouse;

-- Show available databases
SHOW DATABASES;

-- Show catalog configuration
DESCRIBE CATALOG iceberg_catalog;

-- MISSING_DOC: Add examples for advanced catalog configurations
-- MISSING_VALIDATION: Add catalog connectivity verification query

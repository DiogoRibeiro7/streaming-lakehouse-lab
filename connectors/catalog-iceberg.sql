-- ============================================================
-- Iceberg REST Catalog Configuration
-- ============================================================
-- Creates an Iceberg catalog using REST catalog service backed
-- by PostgreSQL for metadata and MinIO (S3) for data storage.
--
-- Usage:
--   \i connectors/catalog-iceberg.sql
--
-- Environment Variables (from .env):
--   POSTGRES_HOST=postgres
--   POSTGRES_PORT=5432
--   POSTGRES_DB=iceberg_catalog
--   POSTGRES_USER=iceberg
--   POSTGRES_PASSWORD=iceberg123
--   MINIO_ROOT_USER=admin
--   MINIO_ROOT_PASSWORD=password123
--   S3_BUCKET=lakehouse
--
-- See: docs/TROUBLESHOOTING.md for configuration issues
-- ============================================================

-- Create Iceberg catalog with REST endpoint
CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://iceberg-rest:8181',
  'warehouse' = 's3a://lakehouse/warehouse',

  -- S3/MinIO configuration
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://minio:9000',
  's3.path-style-access' = 'true',  -- Required for MinIO
  's3.access-key-id' = 'admin',
  's3.secret-access-key' = 'password123'
);

-- Alternative: JDBC-based catalog (without REST service)
-- Uncomment to use direct JDBC connection instead of REST:
/*
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
*/

-- Set as default catalog
USE CATALOG iceberg_catalog;

-- Create default namespace
CREATE DATABASE IF NOT EXISTS streaming_lakehouse
COMMENT 'Default namespace for streaming lakehouse tables';

USE streaming_lakehouse;

-- Verify catalog is ready
SHOW DATABASES;

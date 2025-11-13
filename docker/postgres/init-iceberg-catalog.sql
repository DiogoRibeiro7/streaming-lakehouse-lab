-- ============================================================
-- PostgreSQL Initialization Script for Iceberg Catalog
-- ============================================================
-- This script initializes the database schema for Apache Iceberg
-- catalog metadata storage
--
-- Iceberg uses PostgreSQL to store table metadata, schema versions,
-- snapshots, and other catalog information
-- ============================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create schema for Iceberg catalog
CREATE SCHEMA IF NOT EXISTS iceberg;

-- Grant permissions to iceberg user
GRANT ALL PRIVILEGES ON SCHEMA iceberg TO iceberg;
GRANT ALL PRIVILEGES ON DATABASE iceberg_catalog TO iceberg;

-- Set default schema
ALTER DATABASE iceberg_catalog SET search_path TO iceberg, public;

-- Create table for catalog metadata (used by Iceberg JDBC catalog)
-- Note: Iceberg will create its own tables, but we can set up
-- the schema structure and indexes for better performance

COMMENT ON SCHEMA iceberg IS 'Apache Iceberg catalog metadata storage';

-- Create a monitoring view for catalog statistics
CREATE OR REPLACE VIEW iceberg.catalog_stats AS
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) AS table_size,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) AS index_size
FROM pg_tables
WHERE schemaname = 'iceberg'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Grant view access
GRANT SELECT ON iceberg.catalog_stats TO iceberg;

-- Create function to clean up old snapshots (utility)
CREATE OR REPLACE FUNCTION iceberg.get_catalog_version()
RETURNS TABLE(version TEXT) AS $$
BEGIN
    RETURN QUERY SELECT 'Iceberg Catalog v1.0.0'::TEXT;
END;
$$ LANGUAGE plpgsql;

-- Log successful initialization
DO $$
BEGIN
    RAISE NOTICE 'Iceberg catalog schema initialized successfully';
    RAISE NOTICE 'Database: iceberg_catalog';
    RAISE NOTICE 'Schema: iceberg';
    RAISE NOTICE 'User: iceberg';
END $$;

-- MISSING_VALIDATION: Add backup/restore procedures
-- MISSING_DOC: Add monitoring queries for catalog health
-- MISSING_TEST: Add data integrity checks

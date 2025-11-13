-- ============================================================
-- Iceberg Table Query Examples
-- ============================================================
-- Example queries for exploring and analyzing data in Iceberg tables
-- These demonstrate Iceberg features like time travel, metadata queries,
-- and partitioning
-- ============================================================

-- Use Iceberg catalog and namespace
USE CATALOG iceberg_catalog;
USE streaming_lakehouse;

-- ============================================================
-- Table Discovery
-- ============================================================

-- List all tables
SHOW TABLES;

-- Describe table schema
DESCRIBE events_iceberg;

-- Show table properties
SHOW CREATE TABLE events_iceberg;

-- ============================================================
-- Basic Queries
-- ============================================================

-- Count total events
SELECT COUNT(*) AS total_events
FROM events_iceberg;

-- Count events by type
SELECT
    event_type,
    COUNT(*) AS event_count
FROM events_iceberg
GROUP BY event_type
ORDER BY event_count DESC;

-- Recent events (last 24 hours)
SELECT
    event_id,
    event_type,
    user_id,
    event_time,
    date_partition
FROM events_iceberg
WHERE event_time > CURRENT_TIMESTAMP - INTERVAL '24' HOUR
ORDER BY event_time DESC
LIMIT 100;

-- ============================================================
-- Partition Queries (leveraging partition pruning)
-- ============================================================

-- Query specific date partition (efficient)
SELECT
    event_type,
    COUNT(*) AS count,
    COUNT(DISTINCT user_id) AS unique_users
FROM events_iceberg
WHERE date_partition = '2025-01-12'
GROUP BY event_type;

-- Query date range
SELECT
    date_partition,
    event_type,
    COUNT(*) AS event_count
FROM events_iceberg
WHERE date_partition BETWEEN '2025-01-01' AND '2025-01-31'
GROUP BY date_partition, event_type
ORDER BY date_partition, event_type;

-- ============================================================
-- Aggregation Queries
-- ============================================================

-- Daily event summary
SELECT
    date_partition,
    COUNT(*) AS total_events,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT event_type) AS unique_event_types,
    MIN(event_time) AS first_event_time,
    MAX(event_time) AS last_event_time
FROM events_iceberg
GROUP BY date_partition
ORDER BY date_partition DESC;

-- Top users by activity
SELECT
    user_id,
    COUNT(*) AS event_count,
    COUNT(DISTINCT event_type) AS event_types,
    MIN(event_time) AS first_seen,
    MAX(event_time) AS last_seen
FROM events_iceberg
GROUP BY user_id
ORDER BY event_count DESC
LIMIT 20;

-- ============================================================
-- Windowed Metrics Queries
-- ============================================================

-- Query aggregated metrics
SELECT
    window_start,
    window_end,
    event_type,
    event_count,
    unique_users
FROM event_metrics_iceberg
WHERE date_partition = CURRENT_DATE
ORDER BY window_start DESC, event_count DESC;

-- Hourly trends
SELECT
    DATE_FORMAT(window_start, 'yyyy-MM-dd HH:00:00') AS hour,
    SUM(event_count) AS total_events,
    SUM(unique_users) AS total_unique_users
FROM event_metrics_iceberg
WHERE window_start > CURRENT_TIMESTAMP - INTERVAL '24' HOUR
GROUP BY DATE_FORMAT(window_start, 'yyyy-MM-dd HH:00:00')
ORDER BY hour DESC;

-- ============================================================
-- Iceberg Metadata Queries
-- ============================================================

-- View table snapshots (time travel capability)
SELECT * FROM events_iceberg.snapshots;

-- View table history
SELECT * FROM events_iceberg.history;

-- View table files
SELECT * FROM events_iceberg.files;

-- View table manifests
SELECT * FROM events_iceberg.manifests;

-- View table partitions
SELECT * FROM events_iceberg.partitions;

-- ============================================================
-- Time Travel Queries
-- ============================================================

-- Query table at specific snapshot
-- SELECT * FROM events_iceberg VERSION AS OF <snapshot-id>;

-- Query table at specific timestamp
-- SELECT * FROM events_iceberg FOR SYSTEM_TIME AS OF TIMESTAMP '2025-01-12 10:00:00';

-- ============================================================
-- Data Quality Checks
-- ============================================================

-- Check for duplicate event IDs
SELECT
    event_id,
    COUNT(*) AS occurrence_count
FROM events_iceberg
GROUP BY event_id
HAVING COUNT(*) > 1;

-- Check for null values
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN event_id IS NULL THEN 1 ELSE 0 END) AS null_event_ids,
    SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS null_user_ids,
    SUM(CASE WHEN event_type IS NULL THEN 1 ELSE 0 END) AS null_event_types
FROM events_iceberg;

-- Check event time distribution
SELECT
    date_partition,
    MIN(event_time) AS min_time,
    MAX(event_time) AS max_time,
    MAX(event_time) - MIN(event_time) AS time_span
FROM events_iceberg
GROUP BY date_partition
ORDER BY date_partition DESC;

-- MISSING_DOC: Add examples for schema evolution queries
-- MISSING_DOC: Add examples for compaction and maintenance operations
-- MISSING_VALIDATION: Add data freshness checks

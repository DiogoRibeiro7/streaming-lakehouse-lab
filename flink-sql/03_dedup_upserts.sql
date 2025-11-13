-- ============================================================
-- Deduplication and Upserts Pipeline
-- ============================================================
-- This pipeline deduplicates enriched tick data and writes to
-- Iceberg using merge-on-read (MOR) upserts for efficient storage.
--
-- Flow:
--   Kafka ticks_enriched (with duplicates)
--     → Deduplicate on (symbol, event_time) keeping latest
--     → Iceberg ticks_clean (upsert mode, MOR)
--
-- Deduplication Strategy:
--   - Group by: (symbol, event_time)
--   - Keep: Latest record by processing time
--   - Window: Session window to handle out-of-order events
--
-- Usage:
--   make sql file=flink-sql/03_dedup_upserts.sql
--
-- Prerequisites:
--   - ticks_enriched topic populated (run 02_stream_enrichment.sql)
--   - Iceberg catalog initialized
-- ============================================================

-- Step 1: Set up Iceberg catalog
\i connectors/catalog-iceberg.sql

USE app;

-- ============================================================
-- Step 2: Create Kafka source for enriched ticks
-- ============================================================
-- This table may contain duplicate events due to:
--   - At-least-once delivery semantics
--   - Multiple producers writing same event
--   - Retry logic in upstream systems

CREATE TABLE IF NOT EXISTS ticks_enriched_source (
  symbol STRING,
  price DOUBLE,
  volume BIGINT,
  event_time TIMESTAMP(3),
  sector STRING,
  industry STRING,
  company_name STRING,
  timestamp_ms BIGINT,

  -- Processing time attribute for deduplication
  proc_time AS PROCTIME(),

  -- Watermark strategy: Allow 30 seconds of out-of-order events
  -- This balances latency vs completeness
  WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'ticks_enriched',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-dedup-consumer',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.ignore-parse-errors' = 'true'
);

-- ============================================================
-- Step 3: Create Iceberg target table with upsert semantics
-- ============================================================
-- Iceberg table with PRIMARY KEY enables upsert behavior
-- Format version 2 uses merge-on-read for efficient updates

CREATE TABLE IF NOT EXISTS ticks_clean (
  symbol STRING,
  event_time TIMESTAMP(3),
  price DOUBLE,
  volume BIGINT,
  sector STRING,
  industry STRING,
  company_name STRING,
  ingestion_time TIMESTAMP(3),

  -- Composite primary key for upsert deduplication
  -- Iceberg will merge records with same (symbol, event_time)
  PRIMARY KEY (symbol, event_time) NOT ENFORCED
) PARTITIONED BY (DATE_FORMAT(event_time, 'yyyy-MM-dd'))
WITH (
  -- Iceberg format version 2 for merge-on-read
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'snappy',

  -- Merge-on-read configuration
  'write.merge.mode' = 'merge-on-read',
  'write.upsert.enabled' = 'true',

  -- Delete file configuration for MOR
  'write.delete.mode' = 'merge-on-read',

  -- Compaction settings (compact every ~100 files)
  'write.target-file-size-bytes' = '134217728'  -- 128MB
);

-- ============================================================
-- Step 4: Deduplication Logic
-- ============================================================
-- Strategy: Use Flink SQL's deduplication syntax
-- Keeps the LAST record per (symbol, event_time) based on proc_time
--
-- How it works:
--   1. Events are grouped by (symbol, event_time)
--   2. Within each group, latest by processing time is kept
--   3. State is retained based on watermark + allowed lateness
--   4. Late events (beyond watermark) are dropped

-- Option A: Using Flink's built-in deduplication (LAST ROW)
INSERT INTO ticks_clean
SELECT
  symbol,
  event_time,
  price,
  volume,
  sector,
  industry,
  company_name,
  CURRENT_TIMESTAMP AS ingestion_time
FROM (
  SELECT
    symbol,
    event_time,
    price,
    volume,
    sector,
    industry,
    company_name,
    -- Deduplication: Keep last row per (symbol, event_time) by proc_time
    ROW_NUMBER() OVER (
      PARTITION BY symbol, event_time
      ORDER BY proc_time DESC
    ) AS row_num
  FROM ticks_enriched_source
)
WHERE row_num = 1;

-- ============================================================
-- Alternative Deduplication Strategies (Commented)
-- ============================================================

-- Option B: Using Flink's DEDUPLICATE syntax (simpler, same result)
-- This is syntactic sugar for the ROW_NUMBER approach above
/*
INSERT INTO ticks_clean
SELECT
  symbol,
  event_time,
  price,
  volume,
  sector,
  industry,
  company_name,
  CURRENT_TIMESTAMP AS ingestion_time
FROM ticks_enriched_source
WHERE ROW_NUMBER() OVER (
  PARTITION BY symbol, event_time
  ORDER BY proc_time DESC
) = 1;
*/

-- Option C: Keep FIRST record instead of LAST
-- Use this if you trust the first arrival more than subsequent ones
/*
INSERT INTO ticks_clean
SELECT
  symbol,
  event_time,
  price,
  volume,
  sector,
  industry,
  company_name,
  CURRENT_TIMESTAMP AS ingestion_time
FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY symbol, event_time
      ORDER BY proc_time ASC  -- ASC for first, DESC for last
    ) AS row_num
  FROM ticks_enriched_source
)
WHERE row_num = 1;
*/

-- ============================================================
-- Watermarking and Late Data Handling Notes
-- ============================================================
--
-- WATERMARK STRATEGY:
--   WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
--
--   - Events with event_time < (max_event_time - 30s) trigger watermark
--   - Watermark advances monotonically as new events arrive
--   - Windows close when watermark passes window end time
--
-- LATE DATA HANDLING:
--   - Late events (event_time < watermark) are DROPPED by default
--   - State for deduplication is kept until watermark advances
--   - Trade-off: Larger interval = more late data accepted, but more state
--
-- EXAMPLE SCENARIO:
--   Time: 10:00:00 - Event A arrives (event_time: 10:00:00)
--   Time: 10:00:05 - Event B arrives (event_time: 10:00:00) [duplicate]
--   Time: 10:00:30 - Watermark = 09:59:30 (max_event_time - 30s)
--   Time: 10:01:00 - Event C arrives (event_time: 10:00:00) [late, dropped]
--
-- PROCESSING TIME vs EVENT TIME:
--   - event_time: Business time (when event occurred)
--   - proc_time: System time (when Flink processes event)
--   - Deduplication uses proc_time to determine "latest" version
--   - Watermark uses event_time to trigger window closure
--
-- STATE RETENTION:
--   - State is kept for events within watermark window (30s)
--   - Old state is automatically cleaned up as watermark advances
--   - Memory usage: ~100 bytes per unique (symbol, event_time) pair
--
-- UPSERT BEHAVIOR:
--   - Iceberg PRIMARY KEY enables upsert semantics
--   - If duplicate (symbol, event_time) reaches Iceberg, it merges
--   - Merge-on-read: Updates are stored as delta files
--   - Compaction: Background process merges deltas into base files
--
-- TUNING RECOMMENDATIONS:
--   1. Adjust watermark interval based on data lateness:
--      - Real-time (low latency): 5-10 seconds
--      - Batch-like (high completeness): 1-5 minutes
--
--   2. Monitor late events:
--      SELECT COUNT(*) FROM source WHERE event_time < CURRENT_WATERMARK();
--
--   3. Compaction frequency (in Iceberg):
--      - More frequent: Better read performance, higher write cost
--      - Less frequent: Lower write cost, slower reads
-- ============================================================

-- ============================================================
-- Verification Queries
-- ============================================================

-- Check for duplicates in source (should find some)
-- Run this in a separate SQL client session:
/*
USE CATALOG iceberg_catalog;
USE app;

-- Count duplicates by symbol and event_time in a tumbling window
SELECT
  symbol,
  event_time,
  COUNT(*) AS duplicate_count
FROM ticks_enriched_source
GROUP BY symbol, event_time
HAVING COUNT(*) > 1
LIMIT 10;
*/

-- Verify clean table has no duplicates (should be unique)
/*
SELECT
  symbol,
  event_time,
  COUNT(*) AS count
FROM ticks_clean
GROUP BY symbol, event_time
HAVING COUNT(*) > 1;
-- Should return 0 rows
*/

-- Compare record counts
/*
SELECT
  'source' AS table_name,
  COUNT(*) AS record_count
FROM ticks_enriched_source
UNION ALL
SELECT
  'clean' AS table_name,
  COUNT(*) AS record_count
FROM ticks_clean;
*/

-- Check Iceberg metadata
/*
SELECT * FROM ticks_clean.snapshots;
SELECT * FROM ticks_clean.files;
*/

-- ============================================================
-- Performance Considerations
-- ============================================================
--
-- SQL DEDUPLICATION:
--   Pros:
--     + Simple, declarative syntax
--     + Flink manages state automatically
--     + Easy to modify and test
--     + No Java code required
--
--   Cons:
--     - Limited control over state management
--     - Fixed deduplication strategy (first/last)
--     - State stored in Flink state backend (memory pressure)
--
-- JAVA STATEFUL JOB:
--   Pros:
--     + Full control over state (TTL, serialization, etc.)
--     + Custom deduplication logic (e.g., merge fields)
--     + Better observability and metrics
--     + Can use RocksDB for large state
--
--   Cons:
--     - More code to write and maintain
--     - Requires Java/Scala knowledge
--     - Longer development cycle
--
-- RECOMMENDATION:
--   - Start with SQL for rapid prototyping
--   - Move to Java if you need:
--     * Custom merge logic (not just first/last)
--     * Fine-grained state TTL control
--     * Complex state access patterns
--     * Very large state (>10GB per operator)
--
-- See docs/DEMOS.md for detailed comparison
-- ============================================================

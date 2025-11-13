# Flink SQL Pipelines

This directory contains production-ready Flink SQL pipelines for the Streaming Lakehouse Lab.

## Pipeline Overview

| Pipeline | Source | Sink | Description |
|----------|--------|------|-------------|
| `01_cdc_pg_to_iceberg.sql` | PostgreSQL CDC | Iceberg | Captures changes from PostgreSQL users table and streams to Iceberg lakehouse |
| `02_stream_enrichment.sql` | Kafka ticks | Kafka ticks_enriched | Enriches real-time stock ticks with dimension data from Iceberg |
| `03_dedup_upserts.sql` | Kafka ticks_enriched | Iceberg ticks_clean | Deduplicates enriched ticks and writes to Iceberg with upsert semantics (merge-on-read) |
| `04_feature_views.sql` | Kafka ticks_enriched | Iceberg features_ticks | Computes sliding window features (15-min window, 1-min hop) for ML applications |

## Prerequisites

```bash
# 1. Start infrastructure
make up

# 2. Seed infrastructure (topics, buckets, catalog)
make seed
```

## Pipeline 01: CDC to Iceberg

Streams PostgreSQL changes to Iceberg tables using Flink CDC.

### Architecture

```
PostgreSQL (users table)
    ↓ (Debezium CDC)
Flink CDC Connector
    ↓ (streaming INSERT/UPDATE/DELETE)
Iceberg Table (iceberg_catalog.app.users)
```

### Running the Pipeline

```bash
# Run the CDC pipeline
make sql file=flink-sql/01_cdc_pg_to_iceberg.sql
```

### Verification

```sql
-- Open Flink SQL Client
make flink-shell

-- Check the data
USE CATALOG iceberg_catalog;
USE app;
SELECT * FROM users;

-- Expected: 2 rows (alice, bob)
```

### Database Structure

- **Catalog**: `iceberg_catalog`
- **Database**: `app`
- **Table**: `users`
  - Columns: `id`, `username`, `email`, `created_at`
  - Primary Key: `id` (not enforced)
  - Partition: `bucket(16, id)` - distributes data across 16 buckets

### Testing Updates

```bash
# Open PostgreSQL shell
make postgres-shell

# In psql, insert a new user:
INSERT INTO users (username, email) VALUES ('charlie', 'charlie@example.com');

# Query Iceberg table to see the new row
```

---

## Pipeline 02: Stream Enrichment

Enriches real-time stock ticks with static dimension data.

### Architecture

```
Kafka (ticks topic)
    ↓ (symbol, price, volume, timestamp_ms)
Stream Join with Iceberg Dimension Table (symbols)
    ↓ (+ sector, industry, company_name)
Kafka (ticks_enriched topic)
```

### Step 1: Bootstrap Dimension Table

```bash
# Populate the symbols dimension table with stock metadata
make sql file=flink-sql/02a_bootstrap_symbols.sql
```

This creates and populates the `symbols` table with 20 stock symbols:
- AAPL, GOOGL, MSFT, TSLA, AMZN, JPM, JNJ, XOM, WMT, V
- PG, NVDA, MA, HD, DIS, BAC, INTC, CSCO, NFLX, PFE

### Step 2: Start Enrichment Pipeline

```bash
# Start the stream enrichment job
make sql file=flink-sql/02_stream_enrichment.sql
```

### Step 3: Generate Test Data

```bash
# Generate 100 sample ticks with 100ms delay between each
bash scripts/generate_ticks.sh 100 100

# Generate 1000 ticks rapidly
bash scripts/generate_ticks.sh 1000 10
```

### Step 4: Verify Enrichment

```bash
# Consume original ticks
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic ticks --from-beginning

# Sample output:
# {"symbol":"AAPL","price":175.32,"volume":1234,"timestamp_ms":1673539200000}

# Consume enriched ticks (with sector information)
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic ticks_enriched --from-beginning

# Sample output:
# {"symbol":"AAPL","price":175.32,"volume":1234,"event_time":"2024-01-12T10:00:00Z",
#  "sector":"Technology","industry":"Consumer Electronics","company_name":"Apple Inc.",
#  "timestamp_ms":1673539200000}
```

### Join Strategies

The pipeline uses a **LEFT JOIN** with the Iceberg dimension table:

```sql
SELECT
  t.symbol,
  t.price,
  t.volume,
  t.event_time,
  COALESCE(s.sector, 'Unknown') AS sector,
  COALESCE(s.industry, 'Unknown') AS industry,
  COALESCE(s.company_name, 'Unknown') AS company_name
FROM ticks_stream t
LEFT JOIN symbols s ON t.symbol = s.symbol;
```

**Alternative strategies** (see comments in SQL file):
- **Temporal Join**: For versioned dimension tables
- **Broadcast Join**: For very small dimensions (<10MB)

### Updating Dimension Data

```sql
-- Open Flink SQL Client
make flink-shell

USE CATALOG iceberg_catalog;
USE app;

-- Add a new symbol
INSERT INTO symbols VALUES
  ('ORCL', 'Technology', 'Enterprise Software', 'Oracle Corporation', CURRENT_TIMESTAMP);

-- Update existing symbol
-- Note: Updates require job restart to take effect
INSERT INTO symbols VALUES
  ('AAPL', 'Technology', 'Consumer Electronics', 'Apple Inc. (Updated)', CURRENT_TIMESTAMP);
```

**Note**: The dimension table is cached by Flink. Changes require a job restart to take effect.

---

## Pipeline 03: Deduplication with Upserts

Removes duplicate events from enriched tick stream and writes to Iceberg with upsert semantics (merge-on-read).

### Architecture

```
Kafka (ticks_enriched topic) [may contain duplicates]
    ↓ (symbol, price, event_time, sector, etc.)
Deduplication Logic (ROW_NUMBER by processing time)
    ↓ (keep latest per symbol + event_time)
Iceberg (ticks_clean table) [merge-on-read upserts]
```

### Why Deduplication is Needed

Duplicates can occur due to:
- **At-least-once delivery**: Kafka/Flink retries
- **Multiple producers**: Race conditions
- **Network issues**: Timeout and retry
- **Upstream bugs**: Application logic errors

### Running the Pipeline

```bash
# Run the deduplication pipeline
make sql file=flink-sql/03_dedup_upserts.sql
```

### How It Works

#### 1. Deduplication Strategy

```sql
-- Keeps LAST record per (symbol, event_time) by processing time
SELECT *,
  ROW_NUMBER() OVER (
    PARTITION BY symbol, event_time
    ORDER BY proc_time DESC  -- Latest by processing time
  ) AS row_num
FROM ticks_enriched_source
WHERE row_num = 1
```

**Key Points:**
- **Partition Key**: `(symbol, event_time)` - uniqueness constraint
- **Order Key**: `proc_time` - determines which duplicate to keep
- **Strategy**: Keep LAST (can change to FIRST by using ASC)

#### 2. Watermarking Configuration

```sql
WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
```

**What This Means:**
- Watermark = `max(event_time) - 30 seconds`
- Events with `event_time < watermark` are considered late
- Late events are **dropped** (not deduplicated)

**Trade-offs:**
- **Larger interval (e.g., 5 minutes)**:
  - ✅ Accepts more late data
  - ❌ Higher memory usage (more state)
  - ❌ Higher latency (windows close later)

- **Smaller interval (e.g., 5 seconds)**:
  - ✅ Lower memory usage
  - ✅ Lower latency
  - ❌ More late data dropped

#### 3. Iceberg Upsert Mode

```sql
CREATE TABLE ticks_clean (
  symbol STRING,
  event_time TIMESTAMP(3),
  price DOUBLE,
  -- ... other fields
  PRIMARY KEY (symbol, event_time) NOT ENFORCED
) WITH (
  'format-version' = '2',
  'write.merge.mode' = 'merge-on-read',
  'write.upsert.enabled' = 'true'
);
```

**Merge-on-Read (MOR) Benefits:**
- **Fast writes**: Updates stored as delta files
- **ACID guarantees**: Iceberg transactions
- **Time travel**: Query historical versions
- **Compaction**: Background process merges deltas

**How MOR Works:**
```
Initial Write:  [base-file-1.parquet] (100 records)
Update 1:       [base-file-1.parquet] + [delta-1.parquet] (5 updates)
Update 2:       [base-file-1.parquet] + [delta-1.parquet] + [delta-2.parquet]
Compaction:     [base-file-2.parquet] (merged 105 records)
```

### Verification

#### Check for Duplicates in Source

```bash
make flink-shell

USE CATALOG iceberg_catalog;
USE app;

-- This should find duplicates in the source stream
SELECT
  symbol,
  event_time,
  COUNT(*) AS duplicate_count
FROM ticks_enriched_source
GROUP BY symbol, event_time
HAVING COUNT(*) > 1
LIMIT 10;
```

#### Verify Clean Table Has No Duplicates

```sql
-- This should return 0 rows (no duplicates)
SELECT
  symbol,
  event_time,
  COUNT(*) AS count
FROM ticks_clean
GROUP BY symbol, event_time
HAVING COUNT(*) > 1;
```

#### Compare Record Counts

```sql
-- Source may have more records due to duplicates
SELECT
  'source' AS table_name,
  COUNT(*) AS record_count
FROM ticks_enriched_source

UNION ALL

SELECT
  'clean' AS table_name,
  COUNT(*) AS record_count
FROM ticks_clean;
```

### Late Data Handling Example

**Scenario:**
```
Time 10:00:00 - Event A arrives (event_time: 10:00:00, proc_time: 10:00:00)
Time 10:00:05 - Event B arrives (event_time: 10:00:00, proc_time: 10:00:05)
                → Duplicate detected, Event B kept (later proc_time)
Time 10:00:30 - Watermark advances to 09:59:30
                → State for event_time < 09:59:30 is cleaned up
Time 10:01:00 - Event C arrives (event_time: 10:00:00, proc_time: 10:01:00)
                → Late event! Dropped (event_time < watermark)
```

**Monitoring Late Events:**

Unfortunately, SQL doesn't provide direct late data metrics. In Java, you could use side outputs:

```java
// Java approach for late data monitoring (see docs/DEMOS.md)
ctx.output(lateDataTag, value);
```

### State Management

**State Size Estimation:**
- ~100 bytes per unique `(symbol, event_time)` pair
- 1 million unique keys ≈ 100MB state
- State is kept for watermark window (30 seconds in this example)

**State Cleanup:**
- Automatic when watermark advances
- Old keys are evicted from state
- Configurable via `table.exec.state.ttl` in flink-conf.yaml

### Performance Tuning

#### Adjust Parallelism

```sql
-- Add at the beginning of SQL file
SET 'parallelism.default' = '4';
```

#### Tune Watermark Interval

```sql
-- More lenient (accept more late data, use more memory)
WATERMARK FOR event_time AS event_time - INTERVAL '5' MINUTE

-- Strict (lower latency, drop more late data)
WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
```

#### Iceberg Compaction

```sql
-- Trigger manual compaction
CALL iceberg_catalog.system.rewrite_data_files('app.ticks_clean');

-- View file statistics
SELECT * FROM ticks_clean.files;
```

### SQL vs Java Comparison

For a detailed comparison of implementing deduplication in SQL vs Java, see **[docs/DEMOS.md](../docs/DEMOS.md)**.

**Quick Summary:**

| Aspect | SQL | Java |
|--------|-----|------|
| Development Time | Minutes | Hours |
| State Control | Limited | Full |
| Custom Logic | First/Last only | Any logic |
| Large State (>10GB) | Not recommended | Excellent |
| Observability | Standard metrics | Custom metrics |

**When to use Java instead of SQL:**
- ✅ Need to merge fields (not just keep first/last)
- ✅ State size > 10GB per operator
- ✅ Need custom metrics and detailed logging
- ✅ Performance-critical path

---

## Pipeline 04: Feature Views for ML

Computes time-based features using sliding windows for machine learning applications.

### Architecture

```
Kafka (ticks_enriched topic)
    ↓ (symbol, price, volume, event_time, sector)
Sliding Window Aggregation (HOP: 15-min window, 1-min slide)
    ↓ (mean, min, max, stddev per symbol)
Iceberg (features_ticks table) [partitioned by date, hour]
```

### Use Cases

- **Price Prediction**: Historical price trends for forecasting
- **Anomaly Detection**: Statistical features for outlier detection
- **Trading Signals**: Real-time feature updates for algorithmic trading
- **Risk Management**: Volatility and volume analysis

### Running the Pipeline

```bash
# Start feature computation pipeline
make sql file=flink-sql/04_feature_views.sql
```

### Window Configuration

**Sliding Window (HOP):**
```sql
HOP(event_time, INTERVAL '1' MINUTE, INTERVAL '15' MINUTE)
```

- **Window Size**: 15 minutes (captures medium-term trends)
- **Slide/Hop**: 1 minute (new features every minute)
- **Overlap**: 14 minutes between consecutive windows

**Window Timeline:**
```
10:00:00 - 10:15:00  [window 1]
10:01:00 - 10:16:00  [window 2, overlaps 14 min with window 1]
10:02:00 - 10:17:00  [window 3, overlaps 14 min with window 2]
...
```

### Features Computed

For each symbol and 15-minute window:

| Feature | Description | SQL |
|---------|-------------|-----|
| `mean_price` | Average price | `AVG(price)` |
| `min_price` | Minimum price | `MIN(price)` |
| `max_price` | Maximum price | `MAX(price)` |
| `stddev_price` | Price volatility | `STDDEV_POP(price)` |
| `price_range` | Price spread | `MAX(price) - MIN(price)` |
| `total_volume` | Cumulative volume | `SUM(volume)` |
| `mean_volume` | Average volume | `AVG(volume)` |
| `tick_count` | Number of ticks | `COUNT(*)` |

### Offline Training vs Online Serving

#### Offline Training (Batch Feature Extraction)

**Use Iceberg Time Travel** to access features at any point in history:

```sql
-- Extract training features for January 2025
SELECT
  window_start,
  symbol,
  mean_price,
  stddev_price,
  total_volume
FROM features_ticks
WHERE
  window_start >= TIMESTAMP '2025-01-01 00:00:00'
  AND window_start < TIMESTAMP '2025-02-01 00:00:00'
ORDER BY symbol, window_start;
```

**Benefits for Training:**
- ✅ **Time Travel**: Access historical features at any snapshot
- ✅ **Partition Pruning**: Fast queries for date ranges
- ✅ **Schema Evolution**: Add features without reprocessing
- ✅ **ACID Guarantees**: Consistent training datasets

#### Time-Travel Snapshot Selection

**Prevent Data Leakage** by ensuring features only use data available at prediction time:

```sql
-- Get features as they existed on Jan 15, 2025 at 3pm
SELECT *
FROM features_ticks
FOR SYSTEM_TIME AS OF TIMESTAMP '2025-01-15 15:00:00'
WHERE symbol = 'AAPL';
```

**Query Specific Snapshot:**
```sql
-- Find snapshot ID
SELECT snapshot_id, committed_at
FROM features_ticks.snapshots
WHERE committed_at <= TIMESTAMP '2025-01-15 15:00:00'
ORDER BY committed_at DESC
LIMIT 1;

-- Query using snapshot ID (reproducible)
SELECT *
FROM features_ticks VERSION AS OF <snapshot-id>
WHERE symbol = 'AAPL';
```

**Why This Matters:**
- Ensures training data matches what was available at prediction time
- Prevents look-ahead bias (data leakage)
- Enables reproducible model training

#### Online Serving (Real-time Feature Lookup)

**Architecture Options:**

| Option | Latency | Pros | Cons | When to Use |
|--------|---------|------|------|-------------|
| **Direct Iceberg** | 100-500ms | Simple, no extra infra | Higher latency | Batch predictions |
| **Feature Store (Redis)** | <10ms | Low latency | Extra infra, eventual consistency | Real-time serving (<10ms) |
| **Materialized View** | 10-50ms | Balanced | Moderate complexity | Near real-time (10-50ms) |

**Direct Iceberg Example (Batch Predictions):**
```sql
-- Get latest features for a symbol
SELECT
  symbol,
  mean_price,
  stddev_price,
  total_volume,
  feature_timestamp
FROM features_ticks
WHERE
  symbol = 'AAPL'
  AND window_start >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR
ORDER BY window_start DESC
LIMIT 1;
```

**Feature Store Export (Low-Latency Serving):**
```
Iceberg features_ticks
    ↓ (ETL job)
Redis/DynamoDB (key-value store)
    ↓ (<10ms lookup)
ML Model Serving API
```

### Feature Freshness Monitoring

Monitor staleness to ensure data quality:

```sql
SELECT
  symbol,
  MAX(window_end) AS latest_feature_time,
  CURRENT_TIMESTAMP AS current_time,
  TIMESTAMPDIFF(MINUTE, MAX(window_end), CURRENT_TIMESTAMP) AS staleness_minutes
FROM features_ticks
GROUP BY symbol
HAVING staleness_minutes > 10  -- Alert if >10 minutes stale
ORDER BY staleness_minutes DESC;
```

**Staleness Calculation:**
- 1-minute slide → features are at most 1 minute stale
- 10-second watermark → allows late data up to 10s
- **Total staleness** = slide + watermark = ~70 seconds

### Verification

```bash
make flink-shell

USE CATALOG iceberg_catalog;
USE app;

-- Count features per symbol
SELECT
  symbol,
  COUNT(*) AS feature_count,
  MIN(window_start) AS first_feature,
  MAX(window_start) AS last_feature
FROM features_ticks
GROUP BY symbol
ORDER BY feature_count DESC;

-- Sample features for AAPL
SELECT
  window_start,
  window_end,
  mean_price,
  stddev_price,
  total_volume,
  tick_count
FROM features_ticks
WHERE symbol = 'AAPL'
ORDER BY window_start DESC
LIMIT 20;

-- View Iceberg snapshots for time travel
SELECT snapshot_id, committed_at, operation
FROM features_ticks.snapshots
ORDER BY committed_at DESC;
```

### Advanced Feature Engineering

**Lag Features (compare to previous window):**
```sql
SELECT
  symbol,
  window_start,
  mean_price,
  LAG(mean_price, 1) OVER (PARTITION BY symbol ORDER BY window_start) AS prev_mean_price,
  mean_price - LAG(mean_price, 1) OVER w AS price_change
FROM features_ticks
WINDOW w AS (PARTITION BY symbol ORDER BY window_start);
```

**Rate of Change:**
```sql
SELECT
  symbol,
  window_start,
  (mean_price - LAG(mean_price, 1) OVER w) / LAG(mean_price, 1) OVER w AS price_pct_change
FROM features_ticks
WINDOW w AS (PARTITION BY symbol ORDER BY window_start);
```

**Rank Within Sector:**
```sql
SELECT
  symbol,
  sector,
  window_start,
  mean_price,
  RANK() OVER (PARTITION BY sector, window_start ORDER BY mean_price DESC) AS rank_in_sector
FROM features_ticks;
```

### Performance Considerations

**Compute Cost:**
- Sliding windows are expensive: 15 overlapping windows per 15 minutes
- For 20 symbols: 20 × 60 features/hour = 1,200 rows/hour
- Trade-off: Window size vs freshness vs compute cost

**Storage Cost:**
- 1-minute slide per symbol = 1,440 rows/hour/symbol
- 20 symbols × 1,440 = 28,800 rows/hour
- With 8 features per row: ~100 KB/hour, ~2.4 MB/day, ~1-5 GB/month (compressed)

**Query Performance:**
- Partitioned by date and hour for fast time-range queries
- Use `WHERE window_start >= '2025-01-01'` for partition pruning
- Primary key on `(symbol, window_start)` enables efficient point lookups

**Tuning Options:**
```sql
-- Adjust parallelism
SET 'parallelism.default' = '8';

-- Larger window, less frequent updates (reduce cost)
HOP(event_time, INTERVAL '5' MINUTE, INTERVAL '30' MINUTE)

-- Smaller window, more frequent updates (higher freshness)
HOP(event_time, INTERVAL '30' SECOND, INTERVAL '5' MINUTE)
```

### Best Practices

1. **Feature Naming**: Use descriptive names with time window suffix
   - `mean_price_15m`, `stddev_price_15m`

2. **Metadata**: Always include `window_start`, `window_end`, `feature_timestamp`
   - Enables debugging and lineage tracking

3. **NULL Handling**: Use `COALESCE` for missing values
   - `COALESCE(AVG(price), 0.0) AS mean_price`

4. **Monitoring**: Alert on:
   - Feature staleness (>5 minutes)
   - NULL/NaN values
   - Extreme outliers

5. **Version Control**: Tag Iceberg snapshots for model training
   - Ensures reproducibility

6. **Backfilling**: Use time travel to backfill features
   - No need to re-run entire pipeline for new models

---

## Common Operations

### List Running Jobs

```bash
make list-jobs

# Or via curl:
curl http://localhost:8081/jobs | python -m json.tool
```

### Cancel a Job

```bash
# Get the job ID from list-jobs
make cancel-job JOB_ID=<job-id>
```

### Monitor Jobs

```bash
# Flink JobManager UI
open http://localhost:8081

# View logs
make logs SERVICE=flink-jobmanager
make logs SERVICE=flink-taskmanager-1
```

### Query Iceberg Tables

```bash
make flink-shell

# In SQL Client:
USE CATALOG iceberg_catalog;
SHOW DATABASES;
USE app;
SHOW TABLES;
SELECT * FROM users;
SELECT * FROM symbols;
```

---

## Performance Tuning

### Adjust Parallelism

```sql
-- At the beginning of your SQL script:
SET 'parallelism.default' = '4';
```

### Checkpoint Configuration

Configured in `infra/flink/conf/flink-conf.yaml`:
```yaml
execution.checkpointing.interval: 60s
execution.checkpointing.mode: EXACTLY_ONCE
```

### Watermark Strategy

Adjust watermark tolerance based on your data:
```sql
-- More lenient watermark (allows 30s late data)
WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND

-- Strict watermark (allows 1s late data)
WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
```

---

## Troubleshooting

### Pipeline Fails to Start

```bash
# Check service health
make health

# View Flink logs
make logs SERVICE=flink-jobmanager
```

### No Data in Iceberg Tables

```bash
# Verify Kafka has data
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic ticks --from-beginning --max-messages 1

# Check Flink job status
make list-jobs
```

### Dimension Join Not Working

```bash
# Verify dimension table has data
make flink-shell
USE CATALOG iceberg_catalog;
USE app;
SELECT COUNT(*) FROM symbols;

# Should return 20 after bootstrap
```

### Permission Errors

```bash
# Ensure connectors directory is copied
ls -la /tmp/flink-sql/connectors/

# Re-run with verbose output
make sql file=flink-sql/02_stream_enrichment.sql
```

For more troubleshooting, see: [docs/TROUBLESHOOTING.md](../docs/TROUBLESHOOTING.md)

---

## Next Steps

1. **Windowed Aggregations**: Create time-windowed aggregations on enriched ticks
2. **Complex Event Processing**: Detect patterns in stock movements
3. **Multiple Sinks**: Write to both Kafka and Iceberg simultaneously
4. **Schema Evolution**: Practice Iceberg schema evolution capabilities
5. **Time Travel**: Query historical data using Iceberg time travel

---

## SQL File Conventions

- `\i connectors/file.sql` - Include connector definitions
- `CREATE TABLE IF NOT EXISTS` - Idempotent table creation
- `USE CATALOG catalog_name` - Switch catalogs
- `USE database_name` - Switch databases
- Comments with `--` for single-line, `/* */` for multi-line

---

## References

- [Apache Flink SQL Documentation](https://flink.apache.org/docs/stable/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Flink CDC Documentation](https://github.com/ververica/flink-cdc-connectors)
- [Kafka Connector Documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/kafka/)

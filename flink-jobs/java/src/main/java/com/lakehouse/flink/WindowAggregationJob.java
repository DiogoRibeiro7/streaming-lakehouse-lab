package com.lakehouse.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Windowed Aggregation Streaming Job.
 *
 * <p>This job demonstrates real-time analytics with time windows:
 * <ul>
 *   <li>Reads events from Kafka</li>
 *   <li>Aggregates metrics in tumbling windows (e.g., 5 minutes)</li>
 *   <li>Computes statistics per event type and user</li>
 *   <li>Writes aggregated results to Iceberg</li>
 * </ul>
 *
 * <p>Use case: Real-time dashboard metrics, monitoring, alerting
 *
 * @author Streaming Lakehouse Lab
 * @version 0.1.0
 */
public class WindowAggregationJob {

    private static final Logger LOG = LoggerFactory.getLogger(WindowAggregationJob.class);

    /**
     * Main entry point for the windowed aggregation job.
     *
     * @param args Command-line arguments (not used)
     * @throws Exception If job execution fails
     */
    public static void main(String[] args) throws Exception {
        LOG.info("Starting Windowed Aggregation streaming job");

        // Load configuration
        JobConfig config = JobConfig.fromEnvironment();
        LOG.info("Loaded configuration: {}", config);

        // Create environments
        StreamExecutionEnvironment env = createStreamEnvironment(config);
        StreamTableEnvironment tableEnv = createTableEnvironment(env);

        // Register catalog and tables
        registerIcebergCatalog(tableEnv, config);
        createKafkaSourceTable(tableEnv, config);
        createAggregationSinkTable(tableEnv);

        // Execute windowed aggregation pipeline
        executeAggregationPipeline(tableEnv);

        LOG.info("Windowed Aggregation job submitted successfully");
    }

    /**
     * Creates the streaming execution environment.
     *
     * @param config Job configuration
     * @return Configured StreamExecutionEnvironment
     */
    private static StreamExecutionEnvironment createStreamEnvironment(JobConfig config) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.getParallelism());
        env.enableCheckpointing(config.getCheckpointInterval());

        LOG.info("Created streaming environment");
        return env;
    }

    /**
     * Creates the Table API environment.
     *
     * @param env Streaming execution environment
     * @return Configured StreamTableEnvironment
     */
    private static StreamTableEnvironment createTableEnvironment(StreamExecutionEnvironment env) {
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        return StreamTableEnvironment.create(env, settings);
    }

    /**
     * Registers Iceberg catalog (reuses same logic as KafkaToIcebergJob).
     *
     * @param tableEnv Table environment
     * @param config Job configuration
     */
    private static void registerIcebergCatalog(StreamTableEnvironment tableEnv, JobConfig config) {
        String catalogName = config.getIcebergCatalogName();

        String catalogDDL = String.format(
                "CREATE CATALOG IF NOT EXISTS %s WITH (\n" +
                        "  'type' = 'iceberg',\n" +
                        "  'catalog-type' = 'jdbc',\n" +
                        "  'uri' = '%s',\n" +
                        "  'jdbc.user' = '%s',\n" +
                        "  'jdbc.password' = '%s',\n" +
                        "  'warehouse' = '%s',\n" +
                        "  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',\n" +
                        "  's3.endpoint' = '%s',\n" +
                        "  's3.path-style-access' = 'true',\n" +
                        "  's3.access-key-id' = '%s',\n" +
                        "  's3.secret-access-key' = '%s'\n" +
                        ")",
                catalogName,
                config.getCatalogJdbcUrl(),
                config.getCatalogJdbcUser(),
                config.getCatalogJdbcPassword(),
                config.getIcebergWarehouse(),
                config.getS3Endpoint(),
                config.getS3AccessKey(),
                config.getS3SecretKey()
        );

        tableEnv.executeSql(catalogDDL);
        tableEnv.useCatalog(catalogName);

        String namespace = config.getIcebergNamespace();
        tableEnv.executeSql(String.format("CREATE DATABASE IF NOT EXISTS %s", namespace));
        tableEnv.useDatabase(namespace);

        LOG.info("Registered Iceberg catalog: {}", catalogName);
    }

    /**
     * Creates Kafka source table (reuses same schema as KafkaToIcebergJob).
     *
     * @param tableEnv Table environment
     * @param config Job configuration
     */
    private static void createKafkaSourceTable(StreamTableEnvironment tableEnv, JobConfig config) {
        String sourceDDL = String.format(
                "CREATE TABLE IF NOT EXISTS kafka_events (\n" +
                        "  event_id STRING,\n" +
                        "  event_type STRING,\n" +
                        "  user_id STRING,\n" +
                        "  timestamp_ms BIGINT,\n" +
                        "  payload STRING,\n" +
                        "  event_time AS TO_TIMESTAMP_LTZ(timestamp_ms, 3),\n" +
                        "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = '%s',\n" +
                        "  'properties.bootstrap.servers' = '%s',\n" +
                        "  'properties.group.id' = 'flink-aggregation-consumer',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'json',\n" +
                        "  'json.fail-on-missing-field' = 'false',\n" +
                        "  'json.ignore-parse-errors' = 'true'\n" +
                        ")",
                config.getKafkaTopicEvents(),
                config.getKafkaBootstrapServers()
        );

        tableEnv.executeSql(sourceDDL);
        LOG.info("Created Kafka source table: kafka_events");
    }

    /**
     * Creates Iceberg sink table for aggregated metrics.
     *
     * <p>Table schema includes:
     * <ul>
     *   <li>window_start: Start of the time window</li>
     *   <li>window_end: End of the time window</li>
     *   <li>event_type: Type of events aggregated</li>
     *   <li>event_count: Number of events in window</li>
     *   <li>unique_users: Count of unique users</li>
     *   <li>processing_time: When aggregation was computed</li>
     * </ul>
     *
     * @param tableEnv Table environment
     */
    private static void createAggregationSinkTable(StreamTableEnvironment tableEnv) {
        String sinkDDL =
                "CREATE TABLE IF NOT EXISTS event_metrics_iceberg (\n" +
                        "  window_start TIMESTAMP(3),\n" +
                        "  window_end TIMESTAMP(3),\n" +
                        "  event_type STRING,\n" +
                        "  event_count BIGINT,\n" +
                        "  unique_users BIGINT,\n" +
                        "  processing_time TIMESTAMP(3),\n" +
                        "  date_partition STRING,\n" +
                        "  PRIMARY KEY (window_start, event_type) NOT ENFORCED\n" +
                        ") PARTITIONED BY (date_partition)\n" +
                        "WITH (\n" +
                        "  'format-version' = '2',\n" +
                        "  'write.format.default' = 'parquet',\n" +
                        "  'write.parquet.compression-codec' = 'snappy'\n" +
                        ")";

        tableEnv.executeSql(sinkDDL);
        LOG.info("Created Iceberg aggregation sink table: event_metrics_iceberg");
    }

    /**
     * Executes the windowed aggregation pipeline.
     *
     * <p>Pipeline logic:
     * <ul>
     *   <li>Group events by 5-minute tumbling windows</li>
     *   <li>Aggregate by event_type</li>
     *   <li>Compute count and unique user count</li>
     *   <li>Write results to Iceberg table</li>
     * </ul>
     *
     * @param tableEnv Table environment
     */
    private static void executeAggregationPipeline(StreamTableEnvironment tableEnv) {
        String insertSQL =
                "INSERT INTO event_metrics_iceberg\n" +
                        "SELECT\n" +
                        "  TUMBLE_START(event_time, INTERVAL '5' MINUTE) AS window_start,\n" +
                        "  TUMBLE_END(event_time, INTERVAL '5' MINUTE) AS window_end,\n" +
                        "  event_type,\n" +
                        "  COUNT(*) AS event_count,\n" +
                        "  COUNT(DISTINCT user_id) AS unique_users,\n" +
                        "  CURRENT_TIMESTAMP AS processing_time,\n" +
                        "  DATE_FORMAT(TUMBLE_START(event_time, INTERVAL '5' MINUTE), 'yyyy-MM-dd') AS date_partition\n" +
                        "FROM kafka_events\n" +
                        "GROUP BY\n" +
                        "  TUMBLE(event_time, INTERVAL '5' MINUTE),\n" +
                        "  event_type";

        tableEnv.executeSql(insertSQL);
        LOG.info("Windowed aggregation pipeline started successfully");
    }
}

// MISSING_VALIDATION: Add late event handling configuration
// MISSING_TEST: Add tests for window aggregation logic
// MISSING_DOC: Add examples of querying aggregated metrics

package com.lakehouse.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka to Iceberg Streaming Job (Java/Flink).
 *
 * <p>This job demonstrates a production-quality streaming pipeline that:
 * <ul>
 *   <li>Reads JSON events from Kafka topics</li>
 *   <li>Processes and transforms streaming data</li>
 *   <li>Writes to Iceberg tables with ACID guarantees</li>
 *   <li>Supports exactly-once processing semantics</li>
 * </ul>
 *
 * <p>Architecture:
 * <pre>
 * [Kafka Topic] -&gt; [Flink Streaming] -&gt; [Iceberg Table] -&gt; [MinIO Storage]
 *                         |
 *                  [PostgreSQL Catalog]
 * </pre>
 *
 * <p>Configuration is loaded from environment variables. See {@link JobConfig} for details.
 *
 * @author Streaming Lakehouse Lab
 * @version 0.1.0
 */
public class KafkaToIcebergJob {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaToIcebergJob.class);

    /**
     * Main entry point for the Kafka to Iceberg streaming job.
     *
     * @param args Command-line arguments (not used)
     * @throws Exception If job execution fails
     */
    public static void main(String[] args) throws Exception {
        LOG.info("Starting Kafka to Iceberg streaming job");

        // Load configuration from environment
        JobConfig config = JobConfig.fromEnvironment();
        LOG.info("Loaded configuration: {}", config);

        // Create Flink execution environment
        StreamExecutionEnvironment env = createStreamEnvironment(config);

        // Create Table API environment
        StreamTableEnvironment tableEnv = createTableEnvironment(env);

        // Register Iceberg catalog
        registerIcebergCatalog(tableEnv, config);

        // Create source and sink tables
        createKafkaSourceTable(tableEnv, config);
        createIcebergSinkTable(tableEnv, config);

        // Execute streaming pipeline
        executeStreamingPipeline(tableEnv);

        LOG.info("Kafka to Iceberg job submitted successfully");
    }

    /**
     * Creates and configures the Flink streaming execution environment.
     *
     * <p>Configuration includes:
     * <ul>
     *   <li>Parallelism settings</li>
     *   <li>Checkpointing for fault tolerance</li>
     *   <li>State backend configuration</li>
     * </ul>
     *
     * @param config Job configuration
     * @return Configured StreamExecutionEnvironment
     */
    private static StreamExecutionEnvironment createStreamEnvironment(JobConfig config) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set parallelism
        env.setParallelism(config.getParallelism());

        // Enable checkpointing for exactly-once semantics
        env.enableCheckpointing(config.getCheckpointInterval());

        LOG.info("Created streaming environment with parallelism={}, checkpoint_interval={}ms",
                config.getParallelism(), config.getCheckpointInterval());

        return env;
    }

    /**
     * Creates the Flink Table API environment for SQL-based processing.
     *
     * @param env Streaming execution environment
     * @return Configured StreamTableEnvironment
     */
    private static StreamTableEnvironment createTableEnvironment(StreamExecutionEnvironment env) {
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        LOG.info("Created Table API environment");
        return tableEnv;
    }

    /**
     * Registers the Iceberg catalog with Flink Table API.
     *
     * <p>The catalog configuration includes:
     * <ul>
     *   <li>JDBC connection to PostgreSQL for metadata</li>
     *   <li>S3/MinIO connection for data files</li>
     *   <li>Warehouse location and namespace</li>
     * </ul>
     *
     * @param tableEnv Table environment
     * @param config Job configuration
     */
    private static void registerIcebergCatalog(StreamTableEnvironment tableEnv, JobConfig config) {
        String catalogName = config.getIcebergCatalogName();

        // Create Iceberg catalog
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

        // Use the catalog
        tableEnv.useCatalog(catalogName);

        // Create and use namespace/database
        String namespace = config.getIcebergNamespace();
        tableEnv.executeSql(String.format("CREATE DATABASE IF NOT EXISTS %s", namespace));
        tableEnv.useDatabase(namespace);

        LOG.info("Registered Iceberg catalog: {}, namespace: {}", catalogName, namespace);
    }

    /**
     * Creates the Kafka source table for reading streaming events.
     *
     * <p>Table schema includes:
     * <ul>
     *   <li>event_id: Unique event identifier</li>
     *   <li>event_type: Event category</li>
     *   <li>user_id: User identifier</li>
     *   <li>timestamp_ms: Event timestamp</li>
     *   <li>payload: JSON event payload</li>
     *   <li>event_time: Computed watermark timestamp</li>
     * </ul>
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
                        "  'properties.group.id' = 'flink-consumer',\n" +
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
     * Creates the Iceberg sink table for storing processed events.
     *
     * <p>Table features:
     * <ul>
     *   <li>ACID transactions</li>
     *   <li>Time travel capabilities</li>
     *   <li>Schema evolution support</li>
     *   <li>Partitioned by date for efficient queries</li>
     * </ul>
     *
     * @param tableEnv Table environment
     * @param config Job configuration
     */
    private static void createIcebergSinkTable(StreamTableEnvironment tableEnv, JobConfig config) {
        String sinkDDL =
                "CREATE TABLE IF NOT EXISTS events_iceberg (\n" +
                        "  event_id STRING,\n" +
                        "  event_type STRING,\n" +
                        "  user_id STRING,\n" +
                        "  event_time TIMESTAMP(3),\n" +
                        "  timestamp_ms BIGINT,\n" +
                        "  payload STRING,\n" +
                        "  processing_time TIMESTAMP(3),\n" +
                        "  date_partition STRING,\n" +
                        "  PRIMARY KEY (event_id) NOT ENFORCED\n" +
                        ") PARTITIONED BY (date_partition)\n" +
                        "WITH (\n" +
                        "  'format-version' = '2',\n" +
                        "  'write.format.default' = 'parquet',\n" +
                        "  'write.metadata.compression-codec' = 'gzip',\n" +
                        "  'write.parquet.compression-codec' = 'snappy'\n" +
                        ")";

        tableEnv.executeSql(sinkDDL);
        LOG.info("Created Iceberg sink table: events_iceberg");
    }

    /**
     * Executes the streaming pipeline that reads from Kafka and writes to Iceberg.
     *
     * <p>Pipeline transformations:
     * <ul>
     *   <li>Read events from Kafka source</li>
     *   <li>Add processing timestamp</li>
     *   <li>Compute date partition for efficient queries</li>
     *   <li>Write to Iceberg with exactly-once semantics</li>
     * </ul>
     *
     * @param tableEnv Table environment
     */
    private static void executeStreamingPipeline(StreamTableEnvironment tableEnv) {
        String insertSQL =
                "INSERT INTO events_iceberg\n" +
                        "SELECT\n" +
                        "  event_id,\n" +
                        "  event_type,\n" +
                        "  user_id,\n" +
                        "  event_time,\n" +
                        "  timestamp_ms,\n" +
                        "  payload,\n" +
                        "  CURRENT_TIMESTAMP AS processing_time,\n" +
                        "  DATE_FORMAT(event_time, 'yyyy-MM-dd') AS date_partition\n" +
                        "FROM kafka_events";

        tableEnv.executeSql(insertSQL);
        LOG.info("Streaming pipeline started successfully");
    }
}

// MISSING_VALIDATION: Add data quality checks before writing to Iceberg
// MISSING_TEST: Add unit and integration tests
// MISSING_DOC: Add Javadoc examples and usage patterns

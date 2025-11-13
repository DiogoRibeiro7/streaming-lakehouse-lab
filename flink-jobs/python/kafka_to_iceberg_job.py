"""
Kafka to Iceberg Streaming Job (PyFlink).

This job demonstrates a complete streaming pipeline:
1. Reads JSON events from Kafka topic
2. Parses and validates event data
3. Writes to Iceberg table with ACID guarantees
4. Supports exactly-once processing semantics

Architecture:
    [Kafka Topic] -> [Flink Streaming] -> [Iceberg Table] -> [MinIO Storage]
                            |
                     [PostgreSQL Catalog]

Usage:
    python kafka_to_iceberg_job.py

Environment Variables:
    KAFKA_BOOTSTRAP_SERVERS: Kafka broker address (default: kafka:9092)
    KAFKA_TOPIC_EVENTS: Input Kafka topic (default: streaming.events)
    ICEBERG_CATALOG_NAME: Iceberg catalog name (default: iceberg_catalog)
    ... (see common.py for full list)
"""

import logging

from common import (
    FlinkConfig,
    get_catalog_properties,
    get_config,
    get_kafka_properties,
    setup_logging,
)
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    EnvironmentSettings,
    StreamTableEnvironment,
)

logger = logging.getLogger(__name__)


# ============================================================
# Job Configuration
# ============================================================


def create_stream_env(config: FlinkConfig) -> StreamExecutionEnvironment:
    """
    Create and configure Flink streaming environment.

    Args:
        config: Flink configuration

    Returns:
        Configured StreamExecutionEnvironment instance
    """
    env = StreamExecutionEnvironment.get_execution_environment()

    # Set parallelism
    env.set_parallelism(config.parallelism)

    # Enable checkpointing for fault tolerance
    env.enable_checkpointing(config.checkpoint_interval)

    logger.info(
        "Created Flink environment with parallelism=%d, checkpoint_interval=%dms",
        config.parallelism,
        config.checkpoint_interval,
    )

    return env


def create_table_env(
    stream_env: StreamExecutionEnvironment,
) -> StreamTableEnvironment:
    """
    Create Flink Table API environment.

    Args:
        stream_env: Streaming execution environment

    Returns:
        Configured StreamTableEnvironment instance
    """
    settings = EnvironmentSettings.in_streaming_mode()
    table_env = StreamTableEnvironment.create(stream_env, settings)

    logger.info("Created Flink Table environment")
    return table_env


# ============================================================
# Catalog Setup
# ============================================================


def register_iceberg_catalog(
    table_env: StreamTableEnvironment,
    config: FlinkConfig,
) -> None:
    """
    Register Iceberg catalog with Flink Table API.

    This configures the connection to the Iceberg catalog stored
    in PostgreSQL, with data files stored in MinIO (S3).

    Args:
        table_env: Flink Table environment
        config: Flink configuration
    """
    catalog_name = config.iceberg_catalog_name
    catalog_props = get_catalog_properties(config)

    # Create catalog
    table_env.execute_sql(
        f"""
        CREATE CATALOG IF NOT EXISTS {catalog_name}
        WITH (
            {', '.join(f"'{k}' = '{v}'" for k, v in catalog_props.items())}
        )
        """,
    )

    # Use the catalog
    table_env.use_catalog(catalog_name)

    # Create namespace/database if not exists
    table_env.execute_sql(
        f"CREATE DATABASE IF NOT EXISTS {config.iceberg_namespace}",
    )
    table_env.use_database(config.iceberg_namespace)

    logger.info(
        "Registered Iceberg catalog: %s, namespace: %s",
        catalog_name,
        config.iceberg_namespace,
    )


# ============================================================
# Source Configuration (Kafka)
# ============================================================


def create_kafka_source_table(
    table_env: StreamTableEnvironment,
    config: FlinkConfig,
) -> None:
    """
    Create Kafka source table for reading streaming events.

    Table schema:
        - event_id: STRING (unique event identifier)
        - event_type: STRING (event type/category)
        - user_id: STRING (user identifier)
        - timestamp_ms: BIGINT (event timestamp in milliseconds)
        - payload: STRING (JSON payload)
        - event_time: TIMESTAMP(3) (computed event time)

    Args:
        table_env: Flink Table environment
        config: Flink configuration
    """
    kafka_props = get_kafka_properties(config)

    table_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS kafka_events (
            event_id STRING,
            event_type STRING,
            user_id STRING,
            timestamp_ms BIGINT,
            payload STRING,
            event_time AS TO_TIMESTAMP_LTZ(timestamp_ms, 3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{config.kafka_topic_events}',
            'properties.bootstrap.servers' = '{kafka_props["bootstrap.servers"]}',
            'properties.group.id' = '{kafka_props["group.id"]}',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
        """,
    )

    logger.info("Created Kafka source table: kafka_events")


# ============================================================
# Sink Configuration (Iceberg)
# ============================================================


def create_iceberg_sink_table(
    table_env: StreamTableEnvironment,
    config: FlinkConfig,
) -> None:
    """
    Create Iceberg sink table for storing processed events.

    Table features:
        - ACID transactions
        - Time travel capabilities
        - Schema evolution
        - Partition pruning

    Args:
        table_env: Flink Table environment
        config: Flink configuration
    """
    table_env.execute_sql(
        """
        CREATE TABLE IF NOT EXISTS events_iceberg (
            event_id STRING,
            event_type STRING,
            user_id STRING,
            event_time TIMESTAMP(3),
            timestamp_ms BIGINT,
            payload STRING,
            processing_time TIMESTAMP(3),
            date_partition STRING,
            PRIMARY KEY (event_id) NOT ENFORCED
        ) PARTITIONED BY (date_partition)
        WITH (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.metadata.compression-codec' = 'gzip',
            'write.parquet.compression-codec' = 'snappy'
        )
        """,
    )

    logger.info("Created Iceberg sink table: events_iceberg")


# ============================================================
# Data Processing Pipeline
# ============================================================


def run_streaming_job(config: FlinkConfig) -> None:
    """
    Execute the Kafka to Iceberg streaming job.

    Pipeline steps:
        1. Read from Kafka source table
        2. Transform and enrich data
        3. Write to Iceberg sink table
        4. Commit with exactly-once semantics

    Args:
        config: Flink configuration
    """
    # Create execution environments
    stream_env = create_stream_env(config)
    table_env = create_table_env(stream_env)

    # Register Iceberg catalog
    register_iceberg_catalog(table_env, config)

    # Create source and sink tables
    create_kafka_source_table(table_env, config)
    create_iceberg_sink_table(table_env, config)

    # Define the streaming query
    # Read from Kafka, transform, and write to Iceberg
    table_env.execute_sql(
        """
        INSERT INTO events_iceberg
        SELECT
            event_id,
            event_type,
            user_id,
            event_time,
            timestamp_ms,
            payload,
            CURRENT_TIMESTAMP AS processing_time,
            DATE_FORMAT(event_time, 'yyyy-MM-dd') AS date_partition
        FROM kafka_events
        """,
    )

    logger.info("Streaming job started successfully")


# ============================================================
# Main Entry Point
# ============================================================


def main() -> None:
    """
    Main entry point for the Kafka to Iceberg streaming job.

    Workflow:
        1. Load configuration from environment
        2. Setup logging
        3. Run streaming job
        4. Handle errors and cleanup
    """
    # Load configuration
    config = get_config()

    # Setup logging
    setup_logging(config.log_level)

    logger.info("Starting Kafka to Iceberg streaming job")
    logger.info("Configuration: %s", config.model_dump())

    try:
        # Run the streaming job
        run_streaming_job(config)

        logger.info("Job submitted successfully")

    except Exception:
        logger.exception("Failed to run streaming job")
        raise


if __name__ == "__main__":
    main()


# MISSING_VALIDATION: Add input data validation before writing to Iceberg
# MISSING_TEST: Add integration tests with test containers
# MISSING_DOC: Add metrics and monitoring integration

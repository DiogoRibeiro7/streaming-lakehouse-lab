"""
Common utilities and configurations for PyFlink jobs.

This module provides shared functionality including:
- Environment configuration loading
- Logging setup
- Flink environment configuration
- Catalog and connector setup
"""

import logging

from pydantic import Field
from pydantic_settings import BaseSettings

# ============================================================
# Configuration Classes
# ============================================================


class FlinkConfig(BaseSettings):
    """Flink job configuration loaded from environment variables."""

    # Kafka configuration
    kafka_bootstrap_servers: str = Field(
        default="kafka:9092",
        alias="KAFKA_BOOTSTRAP_SERVERS",
    )
    kafka_topic_events: str = Field(default="streaming.events", alias="KAFKA_TOPIC_EVENTS")
    kafka_topic_metrics: str = Field(default="streaming.metrics", alias="KAFKA_TOPIC_METRICS")
    kafka_group_id: str = Field(default="flink-consumer", alias="KAFKA_GROUP_ID")

    # Iceberg configuration
    iceberg_catalog_name: str = Field(default="iceberg_catalog", alias="ICEBERG_CATALOG_NAME")
    iceberg_warehouse: str = Field(
        default="s3a://lakehouse/warehouse",
        alias="ICEBERG_WAREHOUSE",
    )
    iceberg_namespace: str = Field(
        default="streaming_lakehouse",
        alias="ICEBERG_NAMESPACE",
    )

    # PostgreSQL (catalog) configuration
    catalog_jdbc_url: str = Field(
        default="jdbc:postgresql://postgres:5432/iceberg_catalog",
        alias="CATALOG_JDBC_URL",
    )
    catalog_jdbc_user: str = Field(default="iceberg", alias="CATALOG_JDBC_USER")
    catalog_jdbc_password: str = Field(default="iceberg123", alias="CATALOG_JDBC_PASSWORD")

    # S3/MinIO configuration
    s3_endpoint: str = Field(default="http://minio:9000", alias="AWS_S3_ENDPOINT")
    s3_access_key: str = Field(default="admin", alias="AWS_ACCESS_KEY_ID")
    s3_secret_key: str = Field(default="password123", alias="AWS_SECRET_ACCESS_KEY")
    s3_path_style_access: bool = Field(default=True, alias="AWS_S3_PATH_STYLE_ACCESS")

    # Flink configuration
    parallelism: int = Field(default=2, alias="FLINK_PARALLELISM")
    checkpoint_interval: int = Field(default=60000, alias="FLINK_CHECKPOINT_INTERVAL")

    # Logging
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    class Config:
        env_file = ".env"
        case_sensitive = False


# ============================================================
# Logging Configuration
# ============================================================


def setup_logging(level: str = "INFO") -> None:
    """
    Configure logging for Flink jobs.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# ============================================================
# Environment Helper Functions
# ============================================================


def get_config() -> FlinkConfig:
    """
    Load and return Flink configuration from environment.

    Returns:
        FlinkConfig instance with loaded configuration

    Example:
        >>> config = get_config()
        >>> print(config.kafka_bootstrap_servers)
        'kafka:9092'
    """
    return FlinkConfig()


def get_catalog_properties(config: FlinkConfig) -> dict[str, str]:
    """
    Generate Iceberg catalog properties for Flink.

    Args:
        config: Flink configuration instance

    Returns:
        Dictionary of catalog properties for Flink Table API

    Example:
        >>> config = get_config()
        >>> props = get_catalog_properties(config)
        >>> print(props['type'])
        'iceberg'
    """
    return {
        "type": "iceberg",
        "catalog-type": "jdbc",
        "uri": config.catalog_jdbc_url,
        "jdbc.user": config.catalog_jdbc_user,
        "jdbc.password": config.catalog_jdbc_password,
        "warehouse": config.iceberg_warehouse,
        "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "s3.endpoint": config.s3_endpoint,
        "s3.path-style-access": str(config.s3_path_style_access).lower(),
        "s3.access-key-id": config.s3_access_key,
        "s3.secret-access-key": config.s3_secret_key,
    }


def get_kafka_properties(config: FlinkConfig) -> dict[str, str]:
    """
    Generate Kafka connector properties for Flink.

    Args:
        config: Flink configuration instance

    Returns:
        Dictionary of Kafka properties

    Example:
        >>> config = get_config()
        >>> props = get_kafka_properties(config)
        >>> print(props['bootstrap.servers'])
        'kafka:9092'
    """
    return {
        "bootstrap.servers": config.kafka_bootstrap_servers,
        "group.id": config.kafka_group_id,
    }


# MISSING_VALIDATION: Add configuration validation on startup
# MISSING_DOC: Add examples for custom catalog configurations
# MISSING_TEST: Add unit tests for configuration loading

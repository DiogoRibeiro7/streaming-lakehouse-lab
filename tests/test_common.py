"""
Unit tests for common utilities module.

Tests configuration loading, catalog properties generation,
and other shared functionality.
"""

import os

# Import the module under test
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "flink-jobs", "python"))

from common import (
    FlinkConfig,
    get_catalog_properties,
    get_config,
    get_kafka_properties,
)


class TestFlinkConfig:
    """Test cases for FlinkConfig class."""

    def test_default_configuration(self) -> None:
        """Test that default configuration values are set correctly."""
        config = FlinkConfig()

        assert config.kafka_bootstrap_servers == "kafka:9092"
        assert config.kafka_topic_events == "streaming.events"
        assert config.iceberg_catalog_name == "iceberg_catalog"
        assert config.parallelism == 2
        assert config.checkpoint_interval == 60000

    @patch.dict(
        os.environ,
        {
            "KAFKA_BOOTSTRAP_SERVERS": "test-kafka:9093",
            "FLINK_PARALLELISM": "4",
        },
    )
    def test_configuration_from_environment(self) -> None:
        """Test that configuration is loaded from environment variables."""
        config = FlinkConfig()

        assert config.kafka_bootstrap_servers == "test-kafka:9093"
        assert config.parallelism == 4

    def test_catalog_properties_generation(self) -> None:
        """Test generation of Iceberg catalog properties."""
        config = FlinkConfig()
        props = get_catalog_properties(config)

        assert props["type"] == "iceberg"
        assert props["catalog-type"] == "jdbc"
        assert "uri" in props
        assert "warehouse" in props
        assert props["io-impl"] == "org.apache.iceberg.aws.s3.S3FileIO"

    def test_kafka_properties_generation(self) -> None:
        """Test generation of Kafka connector properties."""
        config = FlinkConfig()
        props = get_kafka_properties(config)

        assert "bootstrap.servers" in props
        assert "group.id" in props
        assert props["bootstrap.servers"] == config.kafka_bootstrap_servers


class TestConfigurationHelpers:
    """Test cases for configuration helper functions."""

    def test_get_config_returns_instance(self) -> None:
        """Test that get_config returns a valid FlinkConfig instance."""
        config = get_config()

        assert isinstance(config, FlinkConfig)
        assert hasattr(config, "kafka_bootstrap_servers")
        assert hasattr(config, "iceberg_catalog_name")

    def test_catalog_properties_contain_required_fields(self) -> None:
        """Test that catalog properties include all required fields."""
        config = get_config()
        props = get_catalog_properties(config)

        required_fields = [
            "type",
            "catalog-type",
            "uri",
            "warehouse",
            "io-impl",
            "s3.endpoint",
            "s3.access-key-id",
            "s3.secret-access-key",
        ]

        for field in required_fields:
            assert field in props, f"Missing required field: {field}"


# MISSING_TEST: Add tests for error handling and validation
# MISSING_TEST: Add integration tests with actual Kafka/Iceberg
# MISSING_TEST: Add tests for edge cases and invalid configurations

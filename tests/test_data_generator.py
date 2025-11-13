"""
Unit tests for sample data generator.

Tests event generation, Kafka producer setup, and data validation.
"""

import json
import os
import sys
from unittest.mock import patch

# Import the module under test
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from generate_sample_data import (
    EVENT_TYPES,
    USER_IDS,
    GeneratorConfig,
    generate_event,
)


class TestEventGeneration:
    """Test cases for event generation functions."""

    def test_generate_event_returns_dict(self) -> None:
        """Test that generate_event returns a dictionary."""
        event = generate_event()

        assert isinstance(event, dict)

    def test_generate_event_has_required_fields(self) -> None:
        """Test that generated events have all required fields."""
        event = generate_event()

        required_fields = ["event_id", "event_type", "user_id", "timestamp_ms", "payload"]

        for field in required_fields:
            assert field in event, f"Missing required field: {field}"

    def test_event_id_is_unique(self) -> None:
        """Test that event IDs are unique."""
        event1 = generate_event()
        event2 = generate_event()

        assert event1["event_id"] != event2["event_id"]

    def test_event_type_is_valid(self) -> None:
        """Test that event type is from the predefined list."""
        event = generate_event()

        assert event["event_type"] in EVENT_TYPES

    def test_user_id_is_valid(self) -> None:
        """Test that user ID is from the predefined list."""
        event = generate_event()

        assert event["user_id"] in USER_IDS

    def test_timestamp_is_numeric(self) -> None:
        """Test that timestamp is a numeric value."""
        event = generate_event()

        assert isinstance(event["timestamp_ms"], int)
        assert event["timestamp_ms"] > 0

    def test_payload_is_json_string(self) -> None:
        """Test that payload is a valid JSON string."""
        event = generate_event()

        assert isinstance(event["payload"], str)

        # Verify it can be parsed as JSON
        payload = json.loads(event["payload"])
        assert isinstance(payload, dict)

    def test_payload_contains_session_info(self) -> None:
        """Test that payload contains session information."""
        event = generate_event()
        payload = json.loads(event["payload"])

        assert "session_id" in payload
        assert "ip_address" in payload
        assert "user_agent" in payload


class TestGeneratorConfig:
    """Test cases for GeneratorConfig class."""

    def test_default_configuration(self) -> None:
        """Test that default configuration values are set."""
        config = GeneratorConfig()

        assert config.kafka_bootstrap_servers == "localhost:9092"
        assert config.kafka_topic_events == "streaming.events"

    @patch.dict(
        os.environ,
        {
            "KAFKA_BOOTSTRAP_SERVERS": "test:9093",
            "KAFKA_TOPIC_EVENTS": "test.topic",
        },
    )
    def test_configuration_from_environment(self) -> None:
        """Test loading configuration from environment."""
        config = GeneratorConfig()

        assert config.kafka_bootstrap_servers == "test:9093"
        assert config.kafka_topic_events == "test.topic"


# MISSING_TEST: Add integration tests with actual Kafka
# MISSING_TEST: Add tests for producer error handling
# MISSING_TEST: Add performance tests for high-rate generation

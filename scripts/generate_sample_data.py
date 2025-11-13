#!/usr/bin/env python3
"""
Generate Sample Data to Kafka.

This script generates realistic sample events and publishes them to Kafka
for testing the streaming lakehouse pipeline.

Usage:
    python scripts/generate_sample_data.py --count 1000 --rate 10

Arguments:
    --count: Number of events to generate (default: 100)
    --rate: Events per second (default: 10)
    --topic: Kafka topic name (default: streaming.events)
"""

import argparse
import json
import logging
import random
import time
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from kafka import KafkaProducer
from kafka.errors import KafkaError
from pydantic import Field
from pydantic_settings import BaseSettings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ============================================================
# Configuration
# ============================================================


class GeneratorConfig(BaseSettings):
    """Configuration for sample data generator."""

    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        alias="KAFKA_BOOTSTRAP_SERVERS",
    )
    kafka_topic_events: str = Field(
        default="streaming.events",
        alias="KAFKA_TOPIC_EVENTS",
    )

    class Config:
        env_file = ".env"
        case_sensitive = False


# ============================================================
# Sample Data Generation
# ============================================================

# Sample event types for realistic data
EVENT_TYPES = [
    "page_view",
    "button_click",
    "form_submit",
    "video_play",
    "video_pause",
    "search",
    "add_to_cart",
    "checkout",
    "purchase",
    "logout",
]

# Sample user IDs (simulating a pool of users)
USER_IDS = [f"user_{i:04d}" for i in range(1, 101)]


def generate_event() -> dict[str, Any]:
    """
    Generate a single sample event with realistic data.

    Returns:
        Dictionary representing an event with fields:
        - event_id: Unique identifier
        - event_type: Type of event
        - user_id: User identifier
        - timestamp_ms: Event timestamp in milliseconds
        - payload: JSON payload with additional data

    Example:
        >>> event = generate_event()
        >>> print(event['event_type'])
        'page_view'
    """
    event_type = random.choice(EVENT_TYPES)
    user_id = random.choice(USER_IDS)

    # Generate payload based on event type
    payload = {
        "session_id": str(uuid4()),
        "ip_address": f"192.168.{random.randint(0, 255)}.{random.randint(1, 254)}",
        "user_agent": "Mozilla/5.0 (compatible; SampleGenerator/1.0)",
    }

    # Add event-specific data
    if event_type in ("page_view", "button_click"):
        payload["url"] = f"/page/{random.randint(1, 100)}"
    elif event_type == "search":
        payload["query"] = f"search_term_{random.randint(1, 50)}"
    elif event_type in ("add_to_cart", "purchase"):
        payload["product_id"] = f"prod_{random.randint(1, 500)}"
        payload["price"] = round(random.uniform(10.0, 500.0), 2)

    return {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "user_id": user_id,
        "timestamp_ms": int(datetime.now(UTC).timestamp() * 1000),
        "payload": json.dumps(payload),
    }


# ============================================================
# Kafka Producer
# ============================================================


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    """
    Create and configure Kafka producer.

    Args:
        bootstrap_servers: Kafka broker addresses

    Returns:
        Configured KafkaProducer instance
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",  # Wait for all replicas to acknowledge
        retries=3,
        max_in_flight_requests_per_connection=1,  # Ensure ordering
    )


def send_event(
    producer: KafkaProducer,
    topic: str,
    event: dict[str, Any],
) -> None:
    """
    Send event to Kafka topic.

    Args:
        producer: Kafka producer instance
        topic: Target Kafka topic
        event: Event data to send

    Raises:
        KafkaError: If sending fails after retries
    """
    try:
        # Use event_id as key for partitioning
        future = producer.send(
            topic,
            key=event["event_id"],
            value=event,
        )

        # Wait for send to complete
        metadata = future.get(timeout=10)

        logger.debug(
            "Sent event %s to partition %d at offset %d",
            event["event_id"],
            metadata.partition,
            metadata.offset,
        )

    except KafkaError:
        logger.exception("Failed to send event: %s", event["event_id"])
        raise


# ============================================================
# Main Generation Loop
# ============================================================


def generate_and_send(
    count: int,
    rate: int,
    topic: str,
    bootstrap_servers: str,
) -> None:
    """
    Generate and send sample events to Kafka.

    Args:
        count: Number of events to generate
        rate: Target events per second
        topic: Kafka topic name
        bootstrap_servers: Kafka broker addresses
    """
    logger.info("Starting sample data generation")
    logger.info("Target: %d events at %d events/sec", count, rate)
    logger.info("Kafka: %s, Topic: %s", bootstrap_servers, topic)

    producer = create_producer(bootstrap_servers)

    try:
        sleep_time = 1.0 / rate if rate > 0 else 0

        for i in range(count):
            # Generate and send event
            event = generate_event()
            send_event(producer, topic, event)

            # Log progress
            if (i + 1) % 100 == 0:
                logger.info("Sent %d/%d events", i + 1, count)

            # Rate limiting
            if sleep_time > 0:
                time.sleep(sleep_time)

        # Flush remaining messages
        producer.flush()

        logger.info("Successfully sent %d events to Kafka", count)

    except KeyboardInterrupt:
        logger.warning("Generation interrupted by user")

    except Exception:
        logger.exception("Error during event generation")
        raise

    finally:
        producer.close()
        logger.info("Producer closed")


# ============================================================
# CLI Interface
# ============================================================


def main() -> None:
    """Main entry point for the sample data generator."""
    parser = argparse.ArgumentParser(
        description="Generate sample events to Kafka for testing",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Number of events to generate (default: 100)",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=10,
        help="Events per second (default: 10)",
    )
    parser.add_argument(
        "--topic",
        type=str,
        help="Kafka topic name (default: from env)",
    )

    args = parser.parse_args()

    # Load configuration
    config = GeneratorConfig()

    # Use topic from args or config
    topic = args.topic or config.kafka_topic_events

    # Generate and send events
    generate_and_send(
        count=args.count,
        rate=args.rate,
        topic=topic,
        bootstrap_servers=config.kafka_bootstrap_servers,
    )


if __name__ == "__main__":
    main()


# MISSING_VALIDATION: Add schema validation for generated events
# MISSING_TEST: Add unit tests for event generation
# MISSING_DOC: Add examples for different event generation patterns

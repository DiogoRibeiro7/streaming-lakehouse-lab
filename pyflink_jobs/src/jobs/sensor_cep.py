"""
Door Sensor CEP Pattern Detection (PyFlink).

This job uses Complex Event Processing (CEP) to detect door open/close sessions
from sensor event streams. It identifies OPEN → CLOSE patterns within a 2-minute
window per device and calculates session durations.

Pattern:
    OPEN event → CLOSE event (within 2 minutes, same device_id)

Architecture:
    [Kafka: sensors] -> [CEP Pattern Match] -> [Session Calculation] -> [Kafka: door_sessions]
                         (by device_id)          (event-time)

Input Schema (JSON):
    {
        "device_id": "door_sensor_001",
        "ts": 1699900000000,
        "event_state": "OPEN"
    }

Output Schema (JSON):
    {
        "device_id": "door_sensor_001",
        "open_ts": 1699900000000,
        "close_ts": 1699900120000,
        "duration_ms": 120000,
        "open_event": {...},
        "close_event": {...}
    }

Environment Variables:
    KAFKA_BOOTSTRAP_SERVERS: Kafka broker address (default: localhost:9092)
    KAFKA_SENSORS_TOPIC: Input topic (default: sensors)
    KAFKA_SESSIONS_TOPIC: Output topic (default: door_sessions)
    KAFKA_GROUP_ID: Consumer group ID (default: sensor-cep-group)
    CEP_PATTERN_TIMEOUT_SEC: Pattern timeout in seconds (default: 120)
    CEP_WATERMARK_LATENESS_SEC: Allowed event lateness in seconds (default: 60)
    FLINK_PARALLELISM: Job parallelism (default: 2)
    FLINK_CHECKPOINT_INTERVAL: Checkpoint interval in ms (default: 60000)

Usage:
    python sensor_cep.py

    # With custom parameters:
    export CEP_PATTERN_TIMEOUT_SEC=180
    export CEP_WATERMARK_LATENESS_SEC=30
    python sensor_cep.py
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

# PyFlink CEP imports
try:
    from pyflink.cep import CEP, Pattern
    from pyflink.cep.pattern import AfterMatchSkipStrategy
    from pyflink.cep.pattern_stream import PatternSelectFunction, PatternTimeoutFunction

    CEP_AVAILABLE = True
except ImportError:
    CEP_AVAILABLE = False

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = logging.getLogger(__name__)

# Constants
OPEN_STATE = "OPEN"
CLOSE_STATE = "CLOSE"
PATTERN_OPEN_NAME = "open"
PATTERN_CLOSE_NAME = "close"


# ============================================================
# Configuration
# ============================================================


@dataclass
class JobConfig:
    """
    Configuration for sensor CEP job.

    Attributes:
        kafka_bootstrap_servers: Kafka broker addresses
        kafka_sensors_topic: Input Kafka topic for sensor events
        kafka_sessions_topic: Output Kafka topic for door sessions
        kafka_group_id: Kafka consumer group ID
        pattern_timeout_sec: Maximum time between OPEN and CLOSE (seconds)
        watermark_lateness_sec: Allowed event lateness for watermarks (seconds)
        parallelism: Flink job parallelism
        checkpoint_interval: Checkpoint interval in milliseconds
    """

    kafka_bootstrap_servers: str
    kafka_sensors_topic: str
    kafka_sessions_topic: str
    kafka_group_id: str
    pattern_timeout_sec: int
    watermark_lateness_sec: int
    parallelism: int
    checkpoint_interval: int

    def __post_init__(self) -> None:
        """Validate configuration parameters."""
        if self.pattern_timeout_sec <= 0:
            msg = f"Pattern timeout must be positive, got {self.pattern_timeout_sec}"
            raise ValueError(msg)
        if self.watermark_lateness_sec < 0:
            msg = f"Watermark lateness cannot be negative, got {self.watermark_lateness_sec}"
            raise ValueError(msg)
        if self.parallelism < 1:
            msg = f"Parallelism must be >= 1, got {self.parallelism}"
            raise ValueError(msg)


def load_config() -> JobConfig:
    """
    Load job configuration from environment variables.

    Returns:
        JobConfig instance with validated parameters

    Raises:
        ValueError: If configuration validation fails
    """
    return JobConfig(
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        kafka_sensors_topic=os.getenv("KAFKA_SENSORS_TOPIC", "sensors"),
        kafka_sessions_topic=os.getenv("KAFKA_SESSIONS_TOPIC", "door_sessions"),
        kafka_group_id=os.getenv("KAFKA_GROUP_ID", "sensor-cep-group"),
        pattern_timeout_sec=int(os.getenv("CEP_PATTERN_TIMEOUT_SEC", "120")),
        watermark_lateness_sec=int(os.getenv("CEP_WATERMARK_LATENESS_SEC", "60")),
        parallelism=int(os.getenv("FLINK_PARALLELISM", "2")),
        checkpoint_interval=int(os.getenv("FLINK_CHECKPOINT_INTERVAL", "60000")),
    )


# ============================================================
# Data Models
# ============================================================


@dataclass
class SensorEvent:
    """
    Door sensor event from Kafka.

    Attributes:
        device_id: Unique sensor/device identifier
        ts: Timestamp in milliseconds (epoch)
        event_state: Event state (OPEN or CLOSE)
    """

    device_id: str
    ts: int
    event_state: str

    @classmethod
    def from_json(cls, json_str: str) -> SensorEvent | None:
        """
        Parse sensor event from JSON string.

        Args:
            json_str: JSON string with sensor event data

        Returns:
            SensorEvent instance or None if parsing fails

        Note:
            MISSING_VALIDATION: Should validate event_state is in allowed set (OPEN/CLOSE).
            Currently accepts any string value which may cause issues downstream.
        """
        try:
            data = json.loads(json_str)
            # Basic type validation
            if not isinstance(data.get("device_id"), str):
                logger.warning("Invalid device_id type: %s", type(data.get("device_id")))
                return None
            if not isinstance(data.get("ts"), (int, float)):
                logger.warning("Invalid ts type: %s", type(data.get("ts")))
                return None
            if not isinstance(data.get("event_state"), str):
                logger.warning("Invalid event_state type: %s", type(data.get("event_state")))
                return None

            # MISSING_VALIDATION: Should validate event_state is OPEN or CLOSE
            # Currently accepts any string, which may lead to patterns never matching
            event_state = data["event_state"].upper()

            return cls(
                device_id=str(data["device_id"]),
                ts=int(data["ts"]),
                event_state=event_state,
            )
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.warning("Failed to parse sensor event: %s - %s", json_str, e)
            return None


@dataclass
class DoorSession:
    """
    Detected door open/close session.

    Attributes:
        device_id: Sensor/device identifier
        open_ts: Timestamp when door opened (ms)
        close_ts: Timestamp when door closed (ms)
        duration_ms: Session duration in milliseconds
        open_event: Original OPEN event data
        close_event: Original CLOSE event data
    """

    device_id: str
    open_ts: int
    close_ts: int
    duration_ms: int
    open_event: dict[str, str | int]
    close_event: dict[str, str | int]

    def __post_init__(self) -> None:
        """
        Validate session data.

        Note:
            MISSING_VALIDATION: Should handle cases where close_ts < open_ts
            (out-of-order events that passed watermark). Currently calculates
            negative duration which may cause issues in downstream processing.
        """
        if self.close_ts < self.open_ts:
            logger.warning(
                "Close timestamp before open timestamp for device %s: "
                "open=%d, close=%d (negative duration: %d ms)",
                self.device_id,
                self.open_ts,
                self.close_ts,
                self.duration_ms,
            )
            # MISSING_VALIDATION: Should we reject or correct this?

    def to_json(self) -> str:
        """
        Serialize session to JSON string.

        Returns:
            JSON string representation of door session
        """
        return json.dumps(asdict(self))


# ============================================================
# Event Time and Watermark Configuration
# ============================================================


class SensorTimestampAssigner(TimestampAssigner):  # type: ignore[misc]
    """
    Assign event timestamps from sensor event data.

    Extracts the timestamp from the SensorEvent.ts field for
    event-time processing.
    """

    def extract_timestamp(self, value: SensorEvent, _record_timestamp: int) -> int:
        """
        Extract event timestamp from sensor event.

        Args:
            value: Sensor event
            _record_timestamp: Kafka record timestamp (not used)

        Returns:
            Event timestamp in milliseconds
        """
        return value.ts


def create_watermark_strategy(config: JobConfig) -> WatermarkStrategy:
    """
    Create watermark strategy with bounded out-of-orderness.

    Allows events to arrive late up to the configured lateness duration.
    Events arriving later than this will be dropped.

    Args:
        config: Job configuration

    Returns:
        Configured WatermarkStrategy instance

    Note:
        MISSING_VALIDATION: Should add monitoring for dropped late events.
        Late events beyond the allowed lateness are silently dropped,
        which could lead to missed patterns if lateness is too strict.
    """
    strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(config.watermark_lateness_sec)
    )
    strategy = strategy.with_timestamp_assigner(SensorTimestampAssigner())

    logger.info(
        "Created watermark strategy: bounded_out_of_orderness=%ds",
        config.watermark_lateness_sec,
    )
    return strategy


# ============================================================
# CEP Pattern Definition
# ============================================================


def create_pattern(config: JobConfig) -> Pattern:
    """
    Create CEP pattern for OPEN → CLOSE detection.

    Pattern:
        1. Detect event with state=OPEN (store as 'open')
        2. Followed by event with state=CLOSE (store as 'close')
        3. Within configured timeout window
        4. Skip to next match after pattern completes

    Args:
        config: Job configuration

    Returns:
        Configured Pattern instance

    Note:
        MISSING_VALIDATION: Pattern doesn't handle repeated OPEN events.
        If device sends OPEN → OPEN → CLOSE, the first OPEN is consumed
        and the second OPEN won't start a new pattern until after the CLOSE.
        This may miss overlapping door sessions.

        MISSING_VALIDATION: No handling of unmatched OPEN events (timeouts).
        If door opens but never closes within timeout, we never emit an
        incomplete session record. May want to track these as anomalies.
    """
    # Define pattern: OPEN followed by CLOSE within timeout
    pattern = (
        Pattern.begin(PATTERN_OPEN_NAME, AfterMatchSkipStrategy.skip_to_next())
        .where(lambda event: event.event_state == OPEN_STATE)
        .next(PATTERN_CLOSE_NAME)
        .where(lambda event: event.event_state == CLOSE_STATE)
        .within(Time.seconds(config.pattern_timeout_sec))
    )

    logger.info(
        "Created CEP pattern: OPEN -> CLOSE within %d seconds",
        config.pattern_timeout_sec,
    )

    return pattern


# ============================================================
# Pattern Match Processing
# ============================================================


class SessionSelectFunction(PatternSelectFunction):  # type: ignore[misc]
    """
    Process matched OPEN → CLOSE patterns into door sessions.

    Extracts the OPEN and CLOSE events from the pattern match,
    calculates session duration, and creates a DoorSession object.
    """

    def select(self, pattern: dict[str, Iterable[SensorEvent]]) -> DoorSession:
        """
        Convert pattern match to door session.

        Args:
            pattern: Map of pattern names to matched events

        Returns:
            DoorSession with calculated duration

        Note:
            MISSING_VALIDATION: Assumes exactly one event per pattern name.
            If pattern matching logic changes to allow multiple events,
            this will break. Should validate list lengths.
        """
        # Extract events from pattern match
        # Pattern should contain exactly one OPEN and one CLOSE event
        open_events = list(pattern[PATTERN_OPEN_NAME])
        close_events = list(pattern[PATTERN_CLOSE_NAME])

        # MISSING_VALIDATION: Should validate we got exactly one of each
        if len(open_events) != 1 or len(close_events) != 1:
            logger.warning(
                "Unexpected pattern match: open_events=%d, close_events=%d",
                len(open_events),
                len(close_events),
            )

        open_event = open_events[0]
        close_event = close_events[0]

        # Calculate session duration
        duration_ms = close_event.ts - open_event.ts

        # Create and return session record
        return DoorSession(
            device_id=open_event.device_id,
            open_ts=open_event.ts,
            close_ts=close_event.ts,
            duration_ms=duration_ms,
            open_event={
                "device_id": open_event.device_id,
                "ts": open_event.ts,
                "event_state": open_event.event_state,
            },
            close_event={
                "device_id": close_event.device_id,
                "ts": close_event.ts,
                "event_state": close_event.event_state,
            },
        )


class SessionTimeoutFunction(PatternTimeoutFunction):  # type: ignore[misc]
    """
    Handle pattern timeouts (OPEN without matching CLOSE).

    Currently not emitting timeout events, but could be extended
    to track incomplete sessions for monitoring or alerting.

    Note:
        MISSING_VALIDATION: Timeout events are not tracked or emitted.
        Door left open beyond timeout window has no visibility.
        Consider emitting timeout records to a separate topic for monitoring.
    """

    def timeout(
        self,
        pattern: dict[str, Iterable[SensorEvent]],
        timeout_timestamp: int,
    ) -> DoorSession | None:
        """
        Process pattern timeout (OPEN without CLOSE).

        Args:
            pattern: Partial pattern match (only OPEN event)
            timeout_timestamp: Timestamp when pattern timed out

        Returns:
            None (timeouts currently ignored)

        Note:
            MISSING_VALIDATION: Should emit incomplete session records
            for monitoring and alerting purposes.
        """
        # Extract the OPEN event that timed out
        open_events = list(pattern.get(PATTERN_OPEN_NAME, []))
        if open_events:
            open_event = open_events[0]
            logger.warning(
                "Pattern timeout for device %s: OPEN at %d, timeout at %d",
                open_event.device_id,
                open_event.ts,
                timeout_timestamp,
            )
        # MISSING_VALIDATION: Not emitting timeout records
        return None


# ============================================================
# Kafka Source/Sink Setup
# ============================================================


def create_kafka_source(config: JobConfig) -> KafkaSource:
    """
    Create Kafka source for sensor events.

    Args:
        config: Job configuration

    Returns:
        Configured KafkaSource instance
    """
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(config.kafka_bootstrap_servers)
        .set_topics(config.kafka_sensors_topic)
        .set_group_id(config.kafka_group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    logger.info(
        "Created Kafka source: topic=%s, bootstrap_servers=%s",
        config.kafka_sensors_topic,
        config.kafka_bootstrap_servers,
    )
    return source


def create_kafka_sink(config: JobConfig) -> KafkaSink:
    """
    Create Kafka sink for door sessions.

    Args:
        config: Job configuration

    Returns:
        Configured KafkaSink instance
    """
    serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(config.kafka_sessions_topic)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(config.kafka_bootstrap_servers)
        .set_record_serializer(serializer)
        .build()
    )

    logger.info(
        "Created Kafka sink: topic=%s, bootstrap_servers=%s",
        config.kafka_sessions_topic,
        config.kafka_bootstrap_servers,
    )
    return sink


# ============================================================
# Job Pipeline
# ============================================================


def build_pipeline(env: StreamExecutionEnvironment, config: JobConfig) -> None:
    """
    Build the CEP pattern detection pipeline.

    Pipeline stages:
        1. Read from Kafka (sensors topic)
        2. Parse JSON to SensorEvent objects
        3. Assign event-time watermarks
        4. Key by device_id
        5. Apply CEP pattern matching (OPEN → CLOSE)
        6. Process matches into DoorSession objects
        7. Serialize to JSON
        8. Write to Kafka (door_sessions topic)

    Args:
        env: Flink execution environment
        config: Job configuration

    Raises:
        RuntimeError: If CEP library is not available
    """
    if not CEP_AVAILABLE:
        msg = "PyFlink CEP library not available. Install with: pip install apache-flink-cep"
        raise RuntimeError(msg)

    # Stage 1: Kafka source
    kafka_source = create_kafka_source(config)
    raw_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),  # Will set after parsing
        "Kafka Sensor Source",
    )

    # Stage 2: Parse JSON and filter invalid records
    sensor_stream = raw_stream.map(
        lambda json_str: SensorEvent.from_json(json_str),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    ).filter(lambda event: event is not None)

    # Stage 3: Assign event-time watermarks
    watermark_strategy = create_watermark_strategy(config)
    sensor_stream = sensor_stream.assign_timestamps_and_watermarks(watermark_strategy)

    # Stage 4: Key by device_id for per-device pattern matching
    keyed_stream = sensor_stream.key_by(lambda event: event.device_id)

    # Stage 5: Create and apply CEP pattern
    pattern = create_pattern(config)
    pattern_stream = CEP.pattern(keyed_stream, pattern)

    # Stage 6: Process pattern matches and timeouts
    # MISSING_VALIDATION: Timeout side output not configured
    # Currently only processing successful matches, not handling timeouts
    session_stream = pattern_stream.select(
        SessionSelectFunction(),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    # Stage 7: Serialize sessions to JSON
    json_stream = session_stream.map(
        lambda session: session.to_json(),
        output_type=Types.STRING(),
    )

    # Stage 8: Kafka sink
    kafka_sink = create_kafka_sink(config)
    json_stream.sink_to(kafka_sink)

    logger.info(
        "Pipeline configured: sensors -> CEP(OPEN->CLOSE, %ds) -> door_sessions",
        config.pattern_timeout_sec,
    )


# ============================================================
# Main Entry Point
# ============================================================


def setup_logging() -> None:
    """Configure logging for the job."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> None:
    """
    Main entry point for sensor CEP job.

    Workflow:
        1. Load configuration from environment
        2. Setup logging
        3. Create Flink execution environment
        4. Configure checkpointing
        5. Build CEP pipeline
        6. Execute job
    """
    setup_logging()

    # Load configuration
    try:
        config = load_config()
        logger.info("Configuration loaded successfully")
        logger.info("  Kafka brokers: %s", config.kafka_bootstrap_servers)
        logger.info("  Input topic: %s", config.kafka_sensors_topic)
        logger.info("  Output topic: %s", config.kafka_sessions_topic)
        logger.info("  Pattern timeout: %d seconds", config.pattern_timeout_sec)
        logger.info("  Watermark lateness: %d seconds", config.watermark_lateness_sec)
        logger.info("  Parallelism: %d", config.parallelism)
    except ValueError:
        logger.exception("Configuration validation failed")
        raise

    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(config.parallelism)

    # Enable checkpointing for fault tolerance
    env.enable_checkpointing(config.checkpoint_interval)
    logger.info("Checkpointing enabled: interval=%dms", config.checkpoint_interval)

    # Build pipeline
    try:
        build_pipeline(env, config)
        logger.info("Pipeline built successfully")
    except Exception:
        logger.exception("Failed to build pipeline")
        raise

    # Execute job
    logger.info("Starting sensor CEP job...")
    try:
        env.execute("Sensor CEP - Door Session Detection")
    except Exception:
        logger.exception("Job execution failed")
        raise


if __name__ == "__main__":
    main()

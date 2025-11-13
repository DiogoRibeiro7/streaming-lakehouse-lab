"""
EWMA-based Anomaly Detection for Sensor Streams (PyFlink).

This job implements real-time anomaly detection using Exponentially Weighted
Moving Average (EWMA) with adaptive variance thresholds on sensor data streams.

Algorithm:
    1. EWMA_t = alpha * x_t + (1-alpha) * EWMA_{t-1}
    2. Var_t = alpha * (x_t - EWMA_t)² + (1-alpha) * Var_{t-1}
    3. Anomaly if |x_t - EWMA_t| > K * sqrt(Var_t)

Architecture:
    [Kafka: sensors] -> [Keyed Stream] -> [EWMA Processor] -> [Kafka: sensors-alerts]
                           (by device_id)      (stateful)

Input Schema (JSON):
    {
        "device_id": "sensor_001",
        "ts": 1699900000000,
        "x": 42.5
    }

Output Schema (JSON):
    {
        "device_id": "sensor_001",
        "ts": 1699900000000,
        "value": 42.5,
        "ewma": 35.2,
        "std_dev": 3.1,
        "threshold": 9.3,
        "is_anomaly": true,
        "detection_time": 1699900000123
    }

Environment Variables:
    KAFKA_BOOTSTRAP_SERVERS: Kafka broker address (default: localhost:9092)
    KAFKA_SENSORS_TOPIC: Input topic (default: sensors)
    KAFKA_ALERTS_TOPIC: Output topic (default: sensors-alerts)
    KAFKA_GROUP_ID: Consumer group ID (default: ewma-ad-group)
    EWMA_ALPHA: Smoothing factor (0 < alpha <= 1, default: 0.3)
    EWMA_K: Threshold multiplier for std dev (default: 3.0)
    FLINK_PARALLELISM: Job parallelism (default: 2)
    FLINK_CHECKPOINT_INTERVAL: Checkpoint interval in ms (default: 60000)

Usage:
    python ewma_ad.py

    # With custom parameters:
    export EWMA_ALPHA=0.2
    export EWMA_K=2.5
    python ewma_ad.py
"""

from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import TypeInformation
from pyflink.datastream import ProcessFunction, RuntimeContext, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.state import ValueState, ValueStateDescriptor

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = logging.getLogger(__name__)

# Constants for anomaly detection
WARMUP_PERIOD = 2  # Minimum observations before flagging anomalies


# ============================================================
# Configuration
# ============================================================


@dataclass
class JobConfig:
    """
    Configuration for EWMA anomaly detection job.

    Attributes:
        kafka_bootstrap_servers: Kafka broker addresses
        kafka_sensors_topic: Input Kafka topic for sensor readings
        kafka_alerts_topic: Output Kafka topic for anomaly alerts
        kafka_group_id: Kafka consumer group ID
        alpha: EWMA smoothing factor (0 < alpha <= 1)
        k: Standard deviation multiplier for anomaly threshold
        parallelism: Flink job parallelism
        checkpoint_interval: Checkpoint interval in milliseconds
    """

    kafka_bootstrap_servers: str
    kafka_sensors_topic: str
    kafka_alerts_topic: str
    kafka_group_id: str
    alpha: float
    k: float
    parallelism: int
    checkpoint_interval: int

    def __post_init__(self) -> None:
        """Validate configuration parameters."""
        if not 0 < self.alpha <= 1:
            msg = f"ALPHA must be in (0, 1], got {self.alpha}"
            raise ValueError(msg)
        if self.k <= 0:
            msg = f"K must be positive, got {self.k}"
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
        kafka_alerts_topic=os.getenv("KAFKA_ALERTS_TOPIC", "sensors-alerts"),
        kafka_group_id=os.getenv("KAFKA_GROUP_ID", "ewma-ad-group"),
        alpha=float(os.getenv("EWMA_ALPHA", "0.3")),
        k=float(os.getenv("EWMA_K", "3.0")),
        parallelism=int(os.getenv("FLINK_PARALLELISM", "2")),
        checkpoint_interval=int(os.getenv("FLINK_CHECKPOINT_INTERVAL", "60000")),
    )


# ============================================================
# Data Models
# ============================================================


@dataclass
class SensorReading:
    """
    Sensor reading from Kafka.

    Attributes:
        device_id: Unique sensor/device identifier
        ts: Timestamp in milliseconds (epoch)
        x: Sensor measurement value
    """

    device_id: str
    ts: int
    x: float

    @classmethod
    def from_json(cls, json_str: str) -> SensorReading | None:
        """
        Parse sensor reading from JSON string.

        Args:
            json_str: JSON string with sensor data

        Returns:
            SensorReading instance or None if parsing fails
        """
        try:
            data = json.loads(json_str)
            # Basic validation
            if not isinstance(data.get("device_id"), str):
                logger.warning("Invalid device_id type: %s", type(data.get("device_id")))
                return None
            if not isinstance(data.get("ts"), (int, float)):
                logger.warning("Invalid ts type: %s", type(data.get("ts")))
                return None
            if not isinstance(data.get("x"), (int, float)):
                logger.warning("Invalid x type: %s", type(data.get("x")))
                return None

            return cls(
                device_id=str(data["device_id"]),
                ts=int(data["ts"]),
                x=float(data["x"]),
            )
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.warning("Failed to parse sensor reading: %s - %s", json_str, e)
            return None


@dataclass
class AnomalyAlert:
    """
    Anomaly detection alert.

    Attributes:
        device_id: Sensor/device identifier
        ts: Original reading timestamp in ms
        value: Observed sensor value
        ewma: Current EWMA estimate
        std_dev: Current standard deviation
        threshold: Anomaly detection threshold (K * std_dev)
        is_anomaly: Whether value is anomalous
        detection_time: Timestamp when anomaly was detected (ms)
    """

    device_id: str
    ts: int
    value: float
    ewma: float
    std_dev: float
    threshold: float
    is_anomaly: bool
    detection_time: int

    def to_json(self) -> str:
        """
        Serialize alert to JSON string.

        Returns:
            JSON string representation of alert
        """
        return json.dumps(
            {
                "device_id": self.device_id,
                "ts": self.ts,
                "value": self.value,
                "ewma": round(self.ewma, 4),
                "std_dev": round(self.std_dev, 4),
                "threshold": round(self.threshold, 4),
                "is_anomaly": self.is_anomaly,
                "detection_time": self.detection_time,
            }
        )


# ============================================================
# EWMA State
# ============================================================


@dataclass
class EWMAState:
    """
    Persistent state for EWMA calculation per device.

    Attributes:
        ewma: Current EWMA value
        variance: Current variance estimate
        count: Number of observations processed
    """

    ewma: float
    variance: float
    count: int


# ============================================================
# EWMA Anomaly Detection Processor
# ============================================================


class EWMAAnomalyDetector(ProcessFunction):  # type: ignore[misc]
    """
    Stateful processor for EWMA-based anomaly detection.

    Maintains per-device EWMA and variance estimates, detecting anomalies
    when observations deviate beyond K standard deviations from the mean.

    State:
        - ewma_state: ValueState[EWMAState] - per-device EWMA statistics
    """

    def __init__(self, alpha: float, k: float) -> None:
        """
        Initialize EWMA anomaly detector.

        Args:
            alpha: EWMA smoothing factor (0 < alpha <= 1)
            k: Standard deviation multiplier for threshold
        """
        self.alpha = alpha
        self.k = k
        self.ewma_state: ValueState | None = None

    def open(self, runtime_context: RuntimeContext) -> None:
        """
        Initialize state when operator starts.

        Args:
            runtime_context: Flink runtime context
        """
        # Define state descriptor for EWMA statistics
        state_descriptor = ValueStateDescriptor(
            "ewma_state",
            TypeInformation.of_type(dict),  # Store as dict for simplicity
        )
        self.ewma_state = runtime_context.get_state(state_descriptor)

    def process_element(
        self,
        reading: SensorReading,
        ctx: ProcessFunction.Context,
    ) -> Iterable[AnomalyAlert]:
        """
        Process sensor reading and detect anomalies using EWMA.

        Args:
            reading: Input sensor reading
            ctx: Process function context

        Yields:
            AnomalyAlert if anomaly detected, or regular status update
        """
        # Retrieve current state for this device (keyed by device_id)
        assert self.ewma_state is not None  # Initialized in open()  # noqa: S101
        state_dict = self.ewma_state.value()

        if state_dict is None:
            # First observation for this device - initialize state
            state = EWMAState(ewma=reading.x, variance=0.0, count=1)
            is_anomaly = False  # First point is never anomalous
        else:
            # Reconstruct state from dict
            state = EWMAState(**state_dict)

            # Update EWMA: EWMA_t = alpha * x_t + (1-alpha) * EWMA_{t-1}
            new_ewma = self.alpha * reading.x + (1 - self.alpha) * state.ewma

            # Update variance: Var_t = alpha * (x_t - EWMA_t)² + (1-alpha) * Var_{t-1}
            squared_error = (reading.x - new_ewma) ** 2
            new_variance = self.alpha * squared_error + (1 - self.alpha) * state.variance

            # Detect anomaly: |x_t - EWMA_t| > K * sqrt(Var_t)
            deviation = abs(reading.x - state.ewma)
            std_dev = math.sqrt(state.variance) if state.variance > 0 else 0.0
            threshold = self.k * std_dev
            is_anomaly = deviation > threshold and state.count > WARMUP_PERIOD

            # Update state for next iteration
            state = EWMAState(ewma=new_ewma, variance=new_variance, count=state.count + 1)

        # Persist updated state
        self.ewma_state.update(
            {
                "ewma": state.ewma,
                "variance": state.variance,
                "count": state.count,
            }
        )

        # Calculate current std dev and threshold for output
        current_std_dev = math.sqrt(state.variance) if state.variance > 0 else 0.0
        current_threshold = self.k * current_std_dev

        # Emit alert (anomaly or normal reading)
        alert = AnomalyAlert(
            device_id=reading.device_id,
            ts=reading.ts,
            value=reading.x,
            ewma=state.ewma,
            std_dev=current_std_dev,
            threshold=current_threshold,
            is_anomaly=is_anomaly,
            detection_time=int(ctx.timestamp() or reading.ts),  # Use event time or reading ts
        )

        yield alert


# ============================================================
# Kafka Source/Sink Setup
# ============================================================


def create_kafka_source(config: JobConfig) -> KafkaSource:
    """
    Create Kafka source for sensor readings.

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
    Create Kafka sink for anomaly alerts.

    Args:
        config: Job configuration

    Returns:
        Configured KafkaSink instance
    """
    serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(config.kafka_alerts_topic)
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
        config.kafka_alerts_topic,
        config.kafka_bootstrap_servers,
    )
    return sink


# ============================================================
# Job Pipeline
# ============================================================


def build_pipeline(env: StreamExecutionEnvironment, config: JobConfig) -> None:
    """
    Build the EWMA anomaly detection pipeline.

    Pipeline stages:
        1. Read from Kafka (sensors topic)
        2. Parse JSON to SensorReading objects
        3. Key by device_id
        4. Apply EWMA anomaly detection (stateful)
        5. Serialize to JSON
        6. Write to Kafka (sensors-alerts topic)

    Args:
        env: Flink execution environment
        config: Job configuration
    """
    # Stage 1: Kafka source
    kafka_source = create_kafka_source(config)
    raw_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),  # Use processing time for simplicity
        "Kafka Sensor Source",
    )

    # Stage 2: Parse JSON and filter invalid records
    sensor_stream = (
        raw_stream.map(
            lambda json_str: SensorReading.from_json(json_str),
            output_type=Types.PICKLED_BYTE_ARRAY(),  # Will be filtered next
        ).filter(lambda reading: reading is not None)  # Remove parse failures
    )

    # Stage 3: Key by device_id for stateful processing
    keyed_stream = sensor_stream.key_by(lambda reading: reading.device_id)

    # Stage 4: Apply EWMA anomaly detection
    detector = EWMAAnomalyDetector(alpha=config.alpha, k=config.k)
    alert_stream = keyed_stream.process(
        detector,
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    # Stage 5: Serialize alerts to JSON
    json_stream = alert_stream.map(
        lambda alert: alert.to_json(),
        output_type=Types.STRING(),
    )

    # Stage 6: Kafka sink
    kafka_sink = create_kafka_sink(config)
    json_stream.sink_to(kafka_sink)

    logger.info(
        "Pipeline configured: sensors -> EWMA(alpha=%.2f, K=%.1f) -> alerts",
        config.alpha,
        config.k,
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
    Main entry point for EWMA anomaly detection job.

    Workflow:
        1. Load configuration from environment
        2. Setup logging
        3. Create Flink execution environment
        4. Configure checkpointing
        5. Build processing pipeline
        6. Execute job
    """
    setup_logging()

    # Load configuration
    try:
        config = load_config()
        logger.info("Configuration loaded successfully")
        logger.info("  Kafka brokers: %s", config.kafka_bootstrap_servers)
        logger.info("  Input topic: %s", config.kafka_sensors_topic)
        logger.info("  Output topic: %s", config.kafka_alerts_topic)
        logger.info("  EWMA alpha: %.3f", config.alpha)
        logger.info("  Threshold K: %.1f", config.k)
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
    logger.info("Starting EWMA anomaly detection job...")
    try:
        env.execute("EWMA Anomaly Detection")
    except Exception:
        logger.exception("Job execution failed")
        raise


if __name__ == "__main__":
    main()

"""
Streaming Lakehouse Lab - PyFlink Jobs Package.

This package contains PyFlink streaming jobs for processing data
from Kafka and writing to Iceberg tables in the lakehouse.

Modules:
    kafka_to_iceberg_job: Streaming job that reads from Kafka and writes to Iceberg
    window_aggregation_job: Windowed aggregation job for real-time analytics
    common: Common utilities and configurations
"""

__version__ = "0.1.0"
__all__ = ["kafka_to_iceberg_job", "common"]

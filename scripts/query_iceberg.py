#!/usr/bin/env python3
"""
Query Iceberg Tables.

This script demonstrates querying Iceberg tables using PyIceberg
to retrieve and analyze data from the lakehouse.

Usage:
    python scripts/query_iceberg.py --table events_iceberg --limit 10
"""

import argparse
import logging
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ============================================================
# Configuration
# ============================================================


class IcebergConfig(BaseSettings):
    """Configuration for Iceberg catalog connection."""

    catalog_jdbc_url: str = Field(
        default="postgresql+psycopg2://iceberg:iceberg123@localhost:5432/iceberg_catalog",
        alias="CATALOG_JDBC_URL",
    )
    iceberg_warehouse: str = Field(
        default="s3a://lakehouse/warehouse",
        alias="ICEBERG_WAREHOUSE",
    )
    iceberg_namespace: str = Field(
        default="streaming_lakehouse",
        alias="ICEBERG_NAMESPACE",
    )
    s3_endpoint: str = Field(
        default="http://localhost:9000",
        alias="AWS_S3_ENDPOINT",
    )
    s3_access_key: str = Field(default="admin", alias="AWS_ACCESS_KEY_ID")
    s3_secret_key: str = Field(default="password123", alias="AWS_SECRET_ACCESS_KEY")

    class Config:
        env_file = ".env"
        case_sensitive = False


# ============================================================
# Iceberg Catalog Operations
# ============================================================


def create_catalog(config: IcebergConfig) -> Any:
    """
    Create and configure Iceberg catalog connection.

    Args:
        config: Iceberg configuration

    Returns:
        Configured Iceberg catalog instance
    """
    # PyIceberg catalog configuration
    catalog = load_catalog(
        "default",
        **{
            "type": "sql",
            "uri": config.catalog_jdbc_url,
            "warehouse": config.iceberg_warehouse,
            "s3.endpoint": config.s3_endpoint,
            "s3.access-key-id": config.s3_access_key,
            "s3.secret-access-key": config.s3_secret_key,
            "s3.path-style-access": "true",
        },
    )

    logger.info("Connected to Iceberg catalog")
    return catalog


def list_namespaces(catalog: Any) -> list[str]:
    """
    List all namespaces in the catalog.

    Args:
        catalog: Iceberg catalog instance

    Returns:
        List of namespace identifiers
    """
    namespaces = catalog.list_namespaces()
    logger.info("Found %d namespace(s)", len(namespaces))
    return [ns[0] for ns in namespaces]


def list_tables(catalog: Any, namespace: str) -> list[str]:
    """
    List all tables in a namespace.

    Args:
        catalog: Iceberg catalog instance
        namespace: Namespace to query

    Returns:
        List of table names
    """
    tables = catalog.list_tables(namespace)
    logger.info("Found %d table(s) in namespace '%s'", len(tables), namespace)
    return [f"{t[0]}.{t[1]}" for t in tables]


def query_table(
    catalog: Any,
    namespace: str,
    table_name: str,
    limit: int = 10,
) -> None:
    """
    Query and display data from an Iceberg table.

    Args:
        catalog: Iceberg catalog instance
        namespace: Table namespace
        table_name: Table name
        limit: Maximum number of rows to display
    """
    try:
        # Load table
        table = catalog.load_table(f"{namespace}.{table_name}")
        logger.info("Loaded table: %s.%s", namespace, table_name)

        # Display table schema
        logger.info("Table schema:")
        for field in table.schema().fields:
            logger.info("  - %s: %s", field.name, field.field_type)

        # Display table properties
        logger.info("Table properties:")
        for key, value in table.properties.items():
            logger.info("  - %s: %s", key, value)

        # Query data (scan table)
        logger.info("Querying table data (limit: %d rows)...", limit)
        scan = table.scan(limit=limit)

        # Display results
        row_count = 0
        for batch in scan.to_arrow():
            logger.info("\nSample data:")
            logger.info(batch.to_pandas().to_string())
            row_count += len(batch)

        logger.info("\nTotal rows retrieved: %d", row_count)

        # Display table statistics
        logger.info("\nTable statistics:")
        logger.info("  - Snapshots: %d", len(list(table.snapshots())))
        logger.info("  - Current snapshot ID: %s", table.current_snapshot().snapshot_id)

    except NoSuchTableError:
        logger.error("Table not found: %s.%s", namespace, table_name)
        raise
    except Exception:
        logger.exception("Error querying table")
        raise


# ============================================================
# CLI Interface
# ============================================================


def main() -> None:
    """Main entry point for Iceberg query tool."""
    parser = argparse.ArgumentParser(
        description="Query Iceberg tables in the lakehouse",
    )
    parser.add_argument(
        "--namespace",
        type=str,
        help="Iceberg namespace (default: from env)",
    )
    parser.add_argument(
        "--table",
        type=str,
        help="Table name to query",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum rows to display (default: 10)",
    )
    parser.add_argument(
        "--list-namespaces",
        action="store_true",
        help="List all namespaces",
    )
    parser.add_argument(
        "--list-tables",
        action="store_true",
        help="List all tables in namespace",
    )

    args = parser.parse_args()

    # Load configuration
    config = IcebergConfig()
    namespace = args.namespace or config.iceberg_namespace

    # Create catalog
    catalog = create_catalog(config)

    try:
        # List namespaces
        if args.list_namespaces:
            namespaces = list_namespaces(catalog)
            logger.info("Namespaces:")
            for ns in namespaces:
                logger.info("  - %s", ns)
            return

        # List tables
        if args.list_tables:
            tables = list_tables(catalog, namespace)
            logger.info("Tables in '%s':", namespace)
            for table in tables:
                logger.info("  - %s", table)
            return

        # Query specific table
        if args.table:
            query_table(catalog, namespace, args.table, args.limit)
        else:
            # Default: list tables if no table specified
            tables = list_tables(catalog, namespace)
            logger.info("Available tables:")
            for table in tables:
                logger.info("  - %s", table)
            logger.info("\nUse --table <name> to query a specific table")

    except Exception:
        logger.exception("Failed to execute query")
        raise


if __name__ == "__main__":
    main()


# MISSING_VALIDATION: Add connection retry logic
# MISSING_TEST: Add integration tests for catalog operations
# MISSING_DOC: Add examples for advanced PyIceberg queries

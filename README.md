# Streaming Lakehouse Lab

[![CI](https://github.com/USERNAME/streaming-lakehouse-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/USERNAME/streaming-lakehouse-lab/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![Java](https://img.shields.io/badge/java-21-orange.svg)](https://adoptium.net/)

A production-ready streaming data lakehouse built on Apache Flink, Kafka, and Apache Iceberg, demonstrating real-time data ingestion, processing, and analytics with ACID guarantees and time-travel capabilities. This project showcases end-to-end streaming pipelines using PyFlink and Java Flink jobs, writing to Iceberg tables stored in S3-compatible MinIO storage with PostgreSQL-backed metadata catalog.

## Quickstart

Get the streaming lakehouse running in 5 minutes:

### Prerequisites

- Docker & Docker Compose v2
- Make
- Python 3.12 + Poetry
- Java 21 (for Java jobs)

### Setup

```bash
# 1. Clone and enter the repository
git clone https://github.com/USERNAME/streaming-lakehouse-lab.git
cd streaming-lakehouse-lab

# 2. Copy environment configuration
cp .env.example .env

# 3. Install Python dependencies
poetry install

# 4. Start all services (Kafka, Flink, Postgres, MinIO, SQL Gateway)
make up

# 5. Initialize infrastructure (topics, buckets, catalog)
make seed
```

### Generate and Process Data

```bash
# Generate stock tick data to Kafka
poetry run python scripts/generate_ticks.py --count 1000 --rate 10

# OR generate sensor data
poetry run python scripts/generate_sample_data.py --count 500

# Submit a PyFlink streaming job (Kafka → Iceberg)
make py job=kafka_to_iceberg_job

# OR submit a Java job
make run-java-job
```

### Query and Explore

```bash
# Open Flink SQL shell
make flink-shell

# Or use Jupyter notebooks
poetry run jupyter notebook notebooks/

# Or query via Flink SQL files
make sql file=flink-sql/01_cdc_pg_to_iceberg.sql
```

### Web UIs

- **Flink Dashboard**: http://localhost:8081
- **MinIO Console**: http://localhost:9001 (admin/password123)
- **Kafka UI**: http://localhost:8080 (if configured)

### Cleanup

```bash
# Stop and remove all containers + volumes
make down
```

## Architecture

```
[Kafka] → [Flink Streaming Jobs] → [Iceberg Tables] → [MinIO (S3)]
                                           ↓
                                    [PostgreSQL Catalog]
```

## Documentation

📚 **Comprehensive guides available in the `docs/` directory:**

- **[ARCHITECTURE.md](docs/ARCHITECTURE.md)** - System architecture, component diagrams, service responsibilities, and data flows
- **[DEMOS.md](docs/DEMOS.md)** - Step-by-step demos for SQL, PyFlink, and Java jobs with runnable examples
- **[TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)** - Common issues, Kafka KRaft setup, SQL Gateway, Iceberg+MinIO configuration, and environment variables reference

## Project Structure

- `infra/` - Docker Compose services and configurations
- `pyflink_jobs/` - Python Flink streaming jobs (Poetry project)
- `flink-java-jobs/` - Java Flink streaming jobs (Gradle project)
- `flink-sql/` - Flink SQL scripts for interactive queries
- `connectors/` - Custom Flink connectors and utilities
- `notebooks/` - Jupyter notebooks for analysis
- `datasets/` - Sample datasets
- `docs/` - Additional documentation

## Development

```bash
# Python setup
cd pyflink_jobs && poetry install

# Java setup
cd flink-java-jobs && ./gradlew build

# Run tests
make test

# Code Quality
# Install pre-commit hooks (runs Ruff, Mypy, and file formatters)
pre-commit install

# Run hooks manually on all files
pre-commit run --all-files

# Run hooks on staged files before committing
git commit  # hooks run automatically
```

## Extending

This project is designed to be extended for your own use cases. Here are common extension points:

### Adding New Data Sources

1. **Create seed data**: Add CSV files to `datasets/` with your reference data
2. **Create generator script**: Follow the pattern in `scripts/generate_ticks.py`
   - Define data schema and generation logic
   - Implement CLI arguments for configuration
   - Add Kafka producer logic
3. **Create Kafka topics**: Update `Makefile` `topics` target or run:
   ```bash
   docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 \
     --create --topic your-topic --partitions 3 --replication-factor 1
   ```

### Adding Streaming Jobs

**PyFlink Jobs** (`pyflink_jobs/src/jobs/`):

```python
# my_custom_job.py
from pyflink.datastream import StreamExecutionEnvironment
# ... implement your job

if __name__ == "__main__":
    main()
```

Run with: `make py job=my_custom_job`

**Java Jobs** (`flink-java-jobs/src/main/java/`):

```java
// MyCustomJob.java
public class MyCustomJob {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = ...;
        // ... implement your job
    }
}
```

Build and run: `./gradlew build && make run-java-job`

### Adding SQL Queries

1. Create SQL file in `flink-sql/` directory
2. Use Iceberg catalog syntax:
   ```sql
   CREATE CATALOG iceberg_catalog WITH (...);
   USE CATALOG iceberg_catalog;
   CREATE DATABASE IF NOT EXISTS my_namespace;
   ```
3. Run with: `make sql file=flink-sql/your_query.sql`

### Adding Notebooks

Add Jupyter notebooks to `notebooks/` for interactive analysis:

- Use `sql_exploration.ipynb` as template for SQL Gateway queries
- Use `iceberg_inspect.ipynb` as template for PyIceberg metadata exploration

### Configuration

Update `.env` for environment-specific settings:

- Kafka topics and partitions
- Resource limits (Flink memory, task slots)
- Storage paths (S3/MinIO buckets)
- Iceberg table properties (file format, compression)

### Monitoring and Observability

Add custom metrics and monitoring:

1. **Flink Metrics**: Use Flink's metric system in jobs
2. **Prometheus**: Configure Flink's Prometheus reporter in `infra/`
3. **Custom Dashboards**: Create Grafana dashboards (add to `infra/`)

### Testing

- **Unit tests**: Add to `tests/` using pytest
- **Integration tests**: Mark with `@pytest.mark.integration`
- **Pre-commit hooks**: Run `make install-hooks` to enforce code quality

## License

Apache License 2.0

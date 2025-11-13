# GitHub Actions CI/CD Workflows

This directory contains GitHub Actions workflows for continuous integration and deployment.

## Workflows

### CI Pipeline (`ci.yml`)

Comprehensive continuous integration pipeline that runs on push and pull requests.

#### Jobs

##### 1. **lint_py** - Python Linting
- **Runtime**: ~5-8 minutes
- **Runner**: ubuntu-latest
- **Python Version**: 3.12
- **Tools**: Poetry, Ruff, Mypy

**Steps**:
1. Checkout code
2. Setup Python 3.12
3. Cache Poetry installation
4. Install Poetry
5. Cache Python virtual environment
6. Install dependencies
7. Run Ruff linter (`ruff check`)
8. Run Ruff formatter check (`ruff format --check`)
9. Run Mypy type checker

**Caching**:
- Poetry installation: `~/.local`
- Virtual environment: `.venv` (keyed by `poetry.lock` hash)

##### 2. **build_java** - Java Build
- **Runtime**: ~10-12 minutes
- **Runner**: ubuntu-latest
- **Java Version**: 21 (Temurin distribution)
- **Build Tool**: Gradle 8.10

**Steps**:
1. Checkout code
2. Setup JDK 21 (Temurin)
3. Setup Gradle with caching
4. Download gradle-wrapper.jar if missing
5. Build root project
6. Build flink-java-jobs subproject
7. Upload build artifacts

**Caching**:
- Gradle wrapper: Managed by `gradle/actions/setup-gradle@v4`
- Gradle dependencies: Automatic via setup-gradle action
- Build cache: Managed by Gradle daemon

**Artifacts**:
- Root JAR: `build/libs/*.jar`
- Flink Java Jobs JAR: `flink-java-jobs/build/libs/*.jar`
- Retention: 7 days

##### 3. **compose_smoke** - Docker Compose Smoke Test
- **Runtime**: ~8-10 minutes
- **Runner**: ubuntu-latest
- **Services**: Kafka, PostgreSQL, MinIO

**Steps**:
1. Checkout code
2. Setup Docker Buildx
3. Start services (`docker compose up -d kafka postgres minio`)
4. Wait for services (120s timeout for Kafka, 60s for Postgres/MinIO)
5. Health check endpoints:
   - Kafka: `kafka-broker-api-versions.sh`
   - PostgreSQL: `pg_isready` + version query
   - MinIO: `mc ready` + admin info
6. Create and verify Kafka topics
7. Create and verify MinIO buckets
8. Verify PostgreSQL database
9. Tear down services (`docker compose down -v`)
10. Verify cleanup (containers stopped, volumes removed)

**Health Checks**:
- Kafka: Broker API versions + topic creation
- PostgreSQL: Connection + database queries
- MinIO: Server readiness + bucket operations

**Cleanup Verification**:
- No containers running with names: kafka, postgres, minio
- No volumes matching pattern: streaming-lakehouse*

##### 4. **summary** - CI Summary
- **Runtime**: <1 minute
- **Depends On**: All previous jobs
- **Purpose**: Aggregate results and create summary

**Outputs**:
- Overall pass/fail status
- Per-job status table in GitHub Step Summary
- Fails if any dependent job failed

## Triggers

The CI workflow runs on:
- **Push** to `main`, `master`, or `develop` branches
- **Pull requests** targeting `main`, `master`, or `develop` branches
- **Manual dispatch** via GitHub Actions UI

## Concurrency

The workflow uses concurrency groups to cancel in-progress runs:
```yaml
concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true
```

This prevents duplicate runs when pushing multiple commits quickly.

## Environment Variables

Global environment variables set for all jobs:
- `PYTHON_VERSION`: "3.12"
- `JAVA_VERSION`: "21"
- `POETRY_VERSION`: "1.8.3"

## Caching Strategy

### Poetry Cache
- **Location**: `~/.local` (Poetry installation)
- **Key**: `poetry-{VERSION}-{OS}-{PYTHON_VERSION}`
- **Location**: `.venv` (Virtual environment)
- **Key**: `venv-{OS}-{PYTHON_VERSION}-{poetry.lock hash}`

### Gradle Cache
- **Managed by**: `gradle/actions/setup-gradle@v4`
- **Cached Items**:
  - Gradle wrapper distribution
  - Dependency caches
  - Build caches
- **Automatic cleanup**: Old cache entries removed

### Docker Layers
- Not explicitly cached (Ubuntu runner has Docker pre-installed)
- Docker Buildx setup for potential future layer caching

## Timeouts

Job-specific timeouts to prevent hung workflows:
- `lint_py`: 10 minutes
- `build_java`: 15 minutes
- `compose_smoke`: 20 minutes (accounts for Docker image pulls)
- `summary`: 5 minutes

## Status Badges

Add to your README.md:

```markdown
[![CI](https://github.com/YOUR_USERNAME/streaming-lakehouse-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/streaming-lakehouse-lab/actions/workflows/ci.yml)
```

## Troubleshooting

### Poetry cache miss
If Poetry dependencies aren't caching properly:
1. Check `poetry.lock` file is committed
2. Verify Poetry version matches `POETRY_VERSION` env var
3. Check cache key includes correct OS and Python version

### Gradle build failures
If Gradle builds fail:
1. Check `gradle-wrapper.jar` exists (auto-downloaded in workflow)
2. Verify Java 21 is being used (`java --version` in logs)
3. Check for dependency resolution issues

### Docker Compose services not ready
If health checks fail:
1. Check service logs in "Display service logs" step
2. Verify `docker/compose.yml` has correct service names
3. Increase timeout values if network is slow
4. Check port conflicts (shouldn't happen on clean runner)

### Cleanup verification fails
If containers/volumes remain after teardown:
1. Check `docker compose down -v` succeeded
2. Verify service names match filter patterns
3. Check for orphaned containers from previous runs

## Local Testing

### Test Python linting locally:
```bash
poetry install
poetry run ruff check flink-jobs/ pyflink_jobs/ tests/ scripts/
poetry run ruff format --check flink-jobs/ pyflink_jobs/ tests/ scripts/
poetry run mypy flink-jobs/ pyflink_jobs/ scripts/
```

### Test Java build locally:
```bash
./gradlew build
./gradlew :flink-java-jobs:build
```

### Test Docker Compose locally:
```bash
# Start services
docker compose -f docker/compose.yml up -d kafka postgres minio

# Health checks
docker exec kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092
docker exec postgres pg_isready -U iceberg
docker exec minio mc ready local

# Cleanup
docker compose -f docker/compose.yml down -v
```

## Performance Optimization

### Current Performance
- **Cold run** (no cache): ~25-30 minutes
- **Warm run** (with cache): ~15-20 minutes

### Future Improvements
1. **Parallel job execution**: Jobs run in parallel by default
2. **Dependency caching**: Already implemented for Poetry and Gradle
3. **Docker layer caching**: Can be added if compose_smoke becomes bottleneck
4. **Artifact reuse**: Build artifacts could be reused across jobs

## Security

### Secrets
No secrets required for current CI workflow. Future additions:
- Container registry credentials (for publishing)
- Cloud credentials (for deployment)
- Signing keys (for releases)

### Permissions
Default permissions (read-only) are sufficient. No elevated permissions needed.

## Contributing

When modifying the CI workflow:
1. Test changes locally first
2. Use feature branches
3. Create small, focused changes
4. Update this README if adding new jobs
5. Test on a fork before merging

## References

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Poetry Actions](https://github.com/snok/install-poetry)
- [Gradle Actions](https://github.com/gradle/actions)
- [Docker Compose](https://docs.docker.com/compose/)

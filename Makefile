# ============================================================
# Streaming Lakehouse Lab - Makefile
# ============================================================
# Provides convenient shortcuts for common development tasks
#
# Usage:
#   make help          Show available commands
#   make up            Start all services
#   make down          Stop all services
#   make logs          View service logs
# ============================================================

.PHONY: help
.DEFAULT_GOAL := help

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m  # No Color

# ------------------------------------------------------------
# Help
# ------------------------------------------------------------
help: ## Show this help message
	@echo "$(BLUE)Streaming Lakehouse Lab - Available Commands$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""

# ------------------------------------------------------------
# Docker Compose Commands
# ------------------------------------------------------------
.PHONY: up down restart ps logs clean

up: ## Start all services (detached mode)
	@echo "$(BLUE)Starting streaming lakehouse stack...$(NC)"
	@echo ""
	@docker compose -f docker/compose.yml up -d
	@echo ""
	@echo "$(GREEN)✓ All services started$(NC)"
	@echo ""
	@echo "$(YELLOW)Service URLs:$(NC)"
	@echo "  • Flink JobManager UI:  http://localhost:8081"
	@echo "  • Flink SQL Gateway:    http://localhost:8083"
	@echo "  • MinIO Console:        http://localhost:9001 (admin/password123)"
	@echo "  • Iceberg REST Catalog: http://localhost:8181"
	@echo "  • PostgreSQL:           localhost:5432 (iceberg/iceberg123)"
	@echo "  • Kafka:                localhost:9092"
	@echo ""
	@echo "$(YELLOW)Next steps:$(NC)"
	@echo "  make seed    # Initialize topics, buckets, and catalog"
	@echo "  make logs    # View service logs"
	@echo "  make help    # See all available commands"
	@echo ""

down: ## Stop all services and remove volumes
	@echo "$(BLUE)Stopping all services and removing volumes...$(NC)"
	@echo ""
	@docker compose -f docker/compose.yml down -v
	@echo ""
	@echo "$(GREEN)✓ Services stopped and volumes removed$(NC)"

restart: down up ## Restart all services

ps: ## List running services
	@docker compose -f docker/compose.yml ps

logs: ## Tail logs for all services (or specify SERVICE=name)
	@echo "$(BLUE)Tailing service logs...$(NC)"
	@echo "$(YELLOW)Tip: Press Ctrl+C to stop$(NC)"
ifdef SERVICE
	@echo "$(YELLOW)Service: $(SERVICE)$(NC)"
	@echo ""
	@docker compose -f docker/compose.yml logs -f $(SERVICE)
else
	@echo ""
	@docker compose -f docker/compose.yml logs -f
endif

clean: down ## Stop services and remove volumes
	@echo "$(YELLOW)Removing all volumes...$(NC)"
	@docker compose -f docker/compose.yml down -v
	@echo "$(GREEN)Cleanup complete.$(NC)"

# ------------------------------------------------------------
# Infrastructure Setup
# ------------------------------------------------------------
.PHONY: topics buckets catalog seed

topics: ## Create Kafka topics (ticks, sensors, cdc.app.public_users)
	@echo "$(BLUE)Creating Kafka topics...$(NC)"
	@echo ""
	@docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic ticks \
		--partitions 3 --replication-factor 1 > /dev/null 2>&1 && \
		echo "  $(GREEN)✓$(NC) Topic 'ticks' created" || \
		echo "  $(YELLOW)⚠$(NC) Topic 'ticks' already exists"
	@docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic sensors \
		--partitions 3 --replication-factor 1 > /dev/null 2>&1 && \
		echo "  $(GREEN)✓$(NC) Topic 'sensors' created" || \
		echo "  $(YELLOW)⚠$(NC) Topic 'sensors' already exists"
	@docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic cdc.app.public_users \
		--partitions 3 --replication-factor 1 > /dev/null 2>&1 && \
		echo "  $(GREEN)✓$(NC) Topic 'cdc.app.public_users' created" || \
		echo "  $(YELLOW)⚠$(NC) Topic 'cdc.app.public_users' already exists"
	@echo ""
	@echo "$(GREEN)Kafka topics ready$(NC)"

buckets: ## Create MinIO warehouse bucket
	@echo "$(BLUE)Creating MinIO buckets...$(NC)"
	@echo ""
	@docker exec minio mc alias set local http://localhost:9000 ${MINIO_ROOT_USER:-admin} ${MINIO_ROOT_PASSWORD:-password123} > /dev/null 2>&1 || true
	@docker exec minio mc mb --ignore-existing local/${S3_BUCKET:-lakehouse}/warehouse > /dev/null 2>&1 && \
		echo "  $(GREEN)✓$(NC) Bucket '${S3_BUCKET:-lakehouse}/warehouse' created" || \
		echo "  $(YELLOW)⚠$(NC) Bucket '${S3_BUCKET:-lakehouse}/warehouse' already exists"
	@echo ""
	@echo "$(GREEN)MinIO buckets ready$(NC)"

catalog: ## Initialize Iceberg REST catalog
	@echo "$(BLUE)Checking Iceberg REST catalog...$(NC)"
	@echo ""
	@curl -sf http://localhost:8181/v1/config > /dev/null 2>&1 && \
		echo "  $(GREEN)✓$(NC) Iceberg REST catalog available at http://localhost:8181" || \
		(echo "  $(RED)✗$(NC) Iceberg REST catalog not available" && \
		 echo "  $(YELLOW)Hint: Run 'make up' to start all services$(NC)" && exit 1)
	@echo ""
	@echo "$(GREEN)Iceberg catalog ready$(NC)"

seed: ## Initialize all infrastructure (topics, buckets, catalog)
	@echo "$(BLUE)Seeding infrastructure...$(NC)"
	@echo "$(BLUE)━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━$(NC)"
	@$(MAKE) -s topics
	@echo ""
	@$(MAKE) -s buckets
	@echo ""
	@$(MAKE) -s catalog
	@echo "$(BLUE)━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━$(NC)"
	@echo "$(GREEN)✓ Infrastructure seeded successfully!$(NC)"

# ------------------------------------------------------------
# Job Submission
# ------------------------------------------------------------
.PHONY: sql py

sql: ## Submit SQL file to Flink SQL client (usage: make sql file=path/to/query.sql)
ifndef file
	@echo ""
	@echo "$(RED)Error: 'file' parameter required$(NC)"
	@echo ""
	@echo "$(YELLOW)Usage:$(NC)"
	@echo "  make sql file=path/to/query.sql"
	@echo ""
	@echo "$(YELLOW)Example:$(NC)"
	@echo "  make sql file=sql/create-iceberg-catalog.sql"
	@echo "  make sql file=flink-sql/01_cdc_pg_to_iceberg.sql"
	@echo ""
	@exit 1
endif
	@echo "$(BLUE)Submitting SQL file to Flink...$(NC)"
	@echo ""
	@if [ ! -f "$(file)" ]; then \
		echo "$(RED)✗ File '$(file)' not found$(NC)"; \
		echo ""; \
		exit 1; \
	fi
	@echo "  $(YELLOW)File:$(NC) $(file)"
	@echo "  $(YELLOW)Target:$(NC) Flink SQL Client"
	@echo ""
	@echo "  $(YELLOW)Preparing execution environment...$(NC)"
	@docker exec flink-jobmanager mkdir -p /tmp/flink-sql 2>/dev/null || true
	@docker exec flink-jobmanager rm -rf /tmp/flink-sql/* 2>/dev/null || true
	@docker cp "$(file)" flink-jobmanager:/tmp/flink-sql/query.sql > /dev/null 2>&1
	@if [ -d "connectors" ]; then \
		docker cp connectors flink-jobmanager:/tmp/flink-sql/ > /dev/null 2>&1 && \
		echo "  $(YELLOW)Copied connector includes$(NC)"; \
	fi
	@echo ""
	@echo "  $(YELLOW)Executing SQL...$(NC)"
	@echo ""
	@docker exec -w /tmp/flink-sql flink-jobmanager /opt/flink/bin/sql-client.sh -f query.sql && \
	echo "" && \
	echo "$(GREEN)✓ SQL executed successfully$(NC)" || \
	(echo "" && echo "$(RED)✗ SQL execution failed$(NC)" && exit 1)

py: ## Run PyFlink job (usage: make py job=module_name)
ifndef job
	@echo ""
	@echo "$(RED)Error: 'job' parameter required$(NC)"
	@echo ""
	@echo "$(YELLOW)Usage:$(NC)"
	@echo "  make py job=module_name"
	@echo ""
	@echo "$(YELLOW)Example:$(NC)"
	@echo "  make py job=kafka_to_iceberg_job"
	@echo "  make py job=ewma_ad"
	@echo ""
	@echo "$(YELLOW)Available PyFlink jobs:$(NC)"
	@find flink-jobs/python -name "*.py" -not -name "__init__.py" -not -name "common.py" 2>/dev/null | \
		sed 's|flink-jobs/python/||' | sed 's|.py$$||' | sed 's|^|  - |' || true
	@find pyflink_jobs/src/jobs -name "*.py" -not -name "__init__.py" 2>/dev/null | \
		sed 's|pyflink_jobs/src/jobs/||' | sed 's|.py$$||' | sed 's|^|  - |' || true
	@echo ""
	@exit 1
endif
	@echo "$(BLUE)Running PyFlink job...$(NC)"
	@echo ""
	@if [ -f "pyflink_jobs/src/jobs/$(job).py" ]; then \
		echo "  $(YELLOW)Job:$(NC) $(job)"; \
		echo "  $(YELLOW)Path:$(NC) pyflink_jobs/src/jobs/$(job).py"; \
		echo ""; \
		PYTHONPATH=pyflink_jobs/src:$$PYTHONPATH poetry run python pyflink_jobs/src/jobs/$(job).py && \
		echo "" && \
		echo "$(GREEN)✓ PyFlink job completed successfully$(NC)" || \
		(echo "" && echo "$(RED)✗ PyFlink job failed$(NC)" && exit 1); \
	elif [ -f "flink-jobs/python/$(job).py" ]; then \
		echo "  $(YELLOW)Job:$(NC) $(job)"; \
		echo "  $(YELLOW)Path:$(NC) flink-jobs/python/$(job).py"; \
		echo ""; \
		poetry run python flink-jobs/python/$(job).py && \
		echo "" && \
		echo "$(GREEN)✓ PyFlink job completed successfully$(NC)" || \
		(echo "" && echo "$(RED)✗ PyFlink job failed$(NC)" && exit 1); \
	else \
		echo "$(RED)✗ Job '$(job)' not found$(NC)"; \
		echo ""; \
		echo "$(YELLOW)Available jobs:$(NC)"; \
		find flink-jobs/python -name "*.py" -not -name "__init__.py" -not -name "common.py" 2>/dev/null | \
			sed 's|flink-jobs/python/||' | sed 's|.py$$||' | sed 's|^|  - |' || true; \
		find pyflink_jobs/src/jobs -name "*.py" -not -name "__init__.py" 2>/dev/null | \
			sed 's|pyflink_jobs/src/jobs/||' | sed 's|.py$$||' | sed 's|^|  - |' || true; \
		echo ""; \
		exit 1; \
	fi

# ------------------------------------------------------------
# Service-specific Commands
# ------------------------------------------------------------
.PHONY: kafka-shell postgres-shell minio-ls flink-shell

kafka-shell: ## Open Kafka console producer (usage: make kafka-shell TOPIC=topic_name)
ifndef TOPIC
	@echo "$(RED)Error: TOPIC parameter required$(NC)"
	@echo "$(YELLOW)Usage: make kafka-shell TOPIC=topic_name$(NC)"
	@echo "$(YELLOW)Example: make kafka-shell TOPIC=ticks$(NC)"
	@exit 1
endif
	@echo "$(BLUE)Opening Kafka console producer for topic: $(TOPIC)$(NC)"
	docker exec -it kafka kafka-console-producer.sh --bootstrap-server localhost:9092 --topic $(TOPIC)

postgres-shell: ## Open PostgreSQL shell
	@echo "$(BLUE)Opening PostgreSQL shell...$(NC)"
	docker exec -it postgres psql -U iceberg -d iceberg_catalog

minio-ls: ## List MinIO buckets and objects
	@echo "$(BLUE)MinIO Contents:$(NC)"
	docker exec minio mc ls local/lakehouse/

flink-shell: ## Open Flink SQL client
	@echo "$(BLUE)Opening Flink SQL client...$(NC)"
	docker exec -it flink-jobmanager ./bin/sql-client.sh

# ------------------------------------------------------------
# Python Development
# ------------------------------------------------------------
.PHONY: install-python lint-python format-python type-check test-python

install-python: ## Install Python dependencies with Poetry
	@echo "$(BLUE)Installing Python dependencies...$(NC)"
	poetry install
	@echo "$(GREEN)Dependencies installed.$(NC)"

lint-python: ## Run Python linter (Ruff)
	@echo "$(BLUE)Running Ruff linter...$(NC)"
	poetry run ruff check flink-jobs/ pyflink_jobs/ tests/ scripts/

format-python: ## Format Python code (Ruff)
	@echo "$(BLUE)Formatting Python code...$(NC)"
	poetry run ruff format flink-jobs/ pyflink_jobs/ tests/ scripts/
	poetry run ruff check --fix flink-jobs/ pyflink_jobs/ tests/ scripts/

type-check: ## Run type checking (MyPy)
	@echo "$(BLUE)Running type checks...$(NC)"
	poetry run mypy flink-jobs/ pyflink_jobs/ scripts/

test-python: ## Run Python tests
	@echo "$(BLUE)Running Python tests...$(NC)"
	poetry run pytest tests/ -v --cov=flink_jobs

# ------------------------------------------------------------
# Java Development
# ------------------------------------------------------------
.PHONY: build-java test-java clean-java

build-java: ## Build Java Flink jobs
	@echo "$(BLUE)Building Java Flink jobs...$(NC)"
	./gradlew clean build shadowJar
	@echo "$(GREEN)Build complete: build/libs/flink-jobs-0.1.0.jar$(NC)"

test-java: ## Run Java tests
	@echo "$(BLUE)Running Java tests...$(NC)"
	./gradlew test

clean-java: ## Clean Java build artifacts
	./gradlew clean

# ------------------------------------------------------------
# Flink Job Submission
# ------------------------------------------------------------
.PHONY: run-python-job run-java-job submit-jar list-jobs cancel-job

run-python-job: ## Run Python Flink job
	@echo "$(BLUE)Submitting Python Flink job...$(NC)"
	poetry run python flink-jobs/python/kafka_to_iceberg_job.py

run-java-job: build-java ## Build and run Java Flink job
	@echo "$(BLUE)Submitting Java Flink job...$(NC)"
	docker exec -it flink-jobmanager /opt/flink/bin/flink run \
		--class com.lakehouse.flink.KafkaToIcebergJob \
		/opt/flink/usrlib/flink-jobs-0.1.0.jar

submit-jar: ## Submit JAR to Flink (specify JAR=path and CLASS=main-class)
ifndef JAR
	@echo "$(RED)Error: JAR variable not set. Usage: make submit-jar JAR=path/to/job.jar CLASS=MainClass$(NC)"
	@exit 1
endif
	docker exec -it flink-jobmanager /opt/flink/bin/flink run --class $(CLASS) $(JAR)

list-jobs: ## List running Flink jobs
	@echo "$(BLUE)Running Flink jobs:$(NC)"
	curl -s http://localhost:8081/jobs | python -m json.tool

cancel-job: ## Cancel a Flink job (specify JOB_ID=id)
ifndef JOB_ID
	@echo "$(RED)Error: JOB_ID not set. Usage: make cancel-job JOB_ID=xxx$(NC)"
	@exit 1
endif
	curl -X PATCH http://localhost:8081/jobs/$(JOB_ID)?mode=cancel

# ------------------------------------------------------------
# Data Generation and Testing
# ------------------------------------------------------------
.PHONY: generate-data query-iceberg

generate-data: ## Generate sample data to Kafka
	@echo "$(BLUE)Generating sample data...$(NC)"
	poetry run python scripts/generate_sample_data.py

query-iceberg: ## Query Iceberg tables (example)
	@echo "$(BLUE)Querying Iceberg tables...$(NC)"
	poetry run python scripts/query_iceberg.py

# ------------------------------------------------------------
# Pre-commit Hooks
# ------------------------------------------------------------
.PHONY: install-hooks run-hooks

install-hooks: ## Install pre-commit hooks
	@echo "$(BLUE)Installing pre-commit hooks...$(NC)"
	poetry run pre-commit install
	poetry run pre-commit install --hook-type commit-msg
	@echo "$(GREEN)Hooks installed.$(NC)"

run-hooks: ## Run pre-commit hooks manually
	@echo "$(BLUE)Running pre-commit hooks...$(NC)"
	poetry run pre-commit run --all-files

# ------------------------------------------------------------
# Integration Tests
# ------------------------------------------------------------
.PHONY: test-integration test-smoke

test-integration: ## Run integration tests
	@echo "$(BLUE)Running integration tests...$(NC)"
	poetry run pytest tests/ -v -m integration

test-smoke: ## Run smoke tests
	@echo "$(BLUE)Running smoke tests...$(NC)"
	bash scripts/smoke_test.sh

# ------------------------------------------------------------
# Monitoring and Health Checks
# ------------------------------------------------------------
.PHONY: health status

health: ## Check health of all services
	@echo "$(BLUE)Checking service health...$(NC)"
	@echo -n "Kafka: "
	@docker exec kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1 && echo "$(GREEN)✓$(NC)" || echo "$(RED)✗$(NC)"
	@echo -n "Flink JobManager: "
	@curl -sf http://localhost:8081/overview > /dev/null && echo "$(GREEN)✓$(NC)" || echo "$(RED)✗$(NC)"
	@echo -n "Flink SQL Gateway: "
	@curl -sf http://localhost:8083/v1/info > /dev/null && echo "$(GREEN)✓$(NC)" || echo "$(RED)✗$(NC)"
	@echo -n "MinIO: "
	@docker exec minio mc ready local > /dev/null 2>&1 && echo "$(GREEN)✓$(NC)" || echo "$(RED)✗$(NC)"
	@echo -n "PostgreSQL: "
	@docker exec postgres pg_isready -U iceberg > /dev/null 2>&1 && echo "$(GREEN)✓$(NC)" || echo "$(RED)✗$(NC)"

status: ps ## Alias for ps command

# ------------------------------------------------------------
# Development Environment
# ------------------------------------------------------------
.PHONY: setup dev-setup

setup: install-python install-hooks ## Complete initial setup
	@echo "$(GREEN)Setup complete! Run 'make up' to start services.$(NC)"

dev-setup: setup up ## Setup and start development environment
	@echo "$(GREEN)Development environment ready!$(NC)"

# ------------------------------------------------------------
# Maintenance
# ------------------------------------------------------------
.PHONY: update-deps prune

update-deps: ## Update Python and Java dependencies
	@echo "$(BLUE)Updating Python dependencies...$(NC)"
	poetry update
	@echo "$(BLUE)Updating Gradle dependencies...$(NC)"
	./gradlew dependencies --refresh-dependencies

prune: ## Remove unused Docker resources
	@echo "$(YELLOW)Pruning Docker resources...$(NC)"
	docker system prune -af --volumes
	@echo "$(GREEN)Prune complete.$(NC)"

# MISSING_VALIDATION: Add performance benchmarking targets
# MISSING_DOC: Add backup/restore targets
# MISSING_TEST: Add end-to-end test targets

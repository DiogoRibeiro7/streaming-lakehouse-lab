#!/usr/bin/env bash
# ============================================================
# Smoke Test Script
# ============================================================
# Validates that all services are running and the streaming
# pipeline is functional
#
# Usage:
#   bash scripts/smoke_test.sh
#
# Exit codes:
#   0 - All tests passed
#   1 - One or more tests failed
# ============================================================

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0

# ============================================================
# Helper Functions
# ============================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[PASS]${NC} $1"
    ((TESTS_PASSED++))
}

log_error() {
    echo -e "${RED}[FAIL]${NC} $1"
    ((TESTS_FAILED++))
}

log_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Check if a service is responding
check_http_endpoint() {
    local name=$1
    local url=$2
    local expected_status=${3:-200}

    log_info "Testing $name at $url"

    if response=$(curl -s -o /dev/null -w "%{http_code}" "$url" 2>&1); then
        if [ "$response" -eq "$expected_status" ]; then
            log_success "$name is responding (HTTP $response)"
            return 0
        else
            log_error "$name returned unexpected status: HTTP $response (expected $expected_status)"
            return 1
        fi
    else
        log_error "$name is not accessible at $url"
        return 1
    fi
}

# Check if a Docker container is running
check_container_running() {
    local container_name=$1

    log_info "Checking if container '$container_name' is running"

    if docker ps --format '{{.Names}}' | grep -q "^${container_name}$"; then
        log_success "Container '$container_name' is running"
        return 0
    else
        log_error "Container '$container_name' is not running"
        return 1
    fi
}

# Check Kafka topics
check_kafka_topics() {
    local topic=$1

    log_info "Checking if Kafka topic '$topic' exists"

    if docker exec kafka kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --list 2>/dev/null | grep -q "^${topic}$"; then
        log_success "Kafka topic '$topic' exists"
        return 0
    else
        log_warning "Kafka topic '$topic' does not exist (will be auto-created)"
        return 0  # Not a failure - topics can be auto-created
    fi
}

# Check PostgreSQL connectivity
check_postgres() {
    log_info "Checking PostgreSQL connectivity"

    if docker exec postgres pg_isready -U iceberg -d iceberg_catalog >/dev/null 2>&1; then
        log_success "PostgreSQL is ready"
        return 0
    else
        log_error "PostgreSQL is not ready"
        return 1
    fi
}

# Check MinIO connectivity
check_minio() {
    log_info "Checking MinIO connectivity"

    if docker exec minio mc ready local >/dev/null 2>&1; then
        log_success "MinIO is ready"
        return 0
    else
        log_error "MinIO is not ready"
        return 1
    fi
}

# ============================================================
# Test Suite
# ============================================================

run_smoke_tests() {
    log_info "Starting smoke tests..."
    echo ""

    # Test 1: Docker containers
    log_info "=== Docker Container Tests ==="
    check_container_running "kafka" || true
    check_container_running "postgres" || true
    check_container_running "minio" || true
    check_container_running "flink-jobmanager" || true
    check_container_running "flink-taskmanager" || true
    check_container_running "flink-sql-gateway" || true
    echo ""

    # Test 2: Service health endpoints
    log_info "=== Service Health Tests ==="
    check_http_endpoint "Flink JobManager" "http://localhost:8081/overview" || true
    check_http_endpoint "Flink SQL Gateway" "http://localhost:8083/v1/info" || true
    check_http_endpoint "Kafka UI" "http://localhost:8080" || true
    check_http_endpoint "MinIO Console" "http://localhost:9001/login" || true
    echo ""

    # Test 3: Database connectivity
    log_info "=== Database Connectivity Tests ==="
    check_postgres || true
    check_minio || true
    echo ""

    # Test 4: Kafka setup
    log_info "=== Kafka Configuration Tests ==="
    check_kafka_topics "streaming.events" || true
    echo ""

    # MISSING_TEST: Add test for Flink job submission
    # MISSING_TEST: Add test for end-to-end data flow
    # MISSING_TEST: Add test for Iceberg table creation
}

# ============================================================
# Summary
# ============================================================

print_summary() {
    echo ""
    echo "============================================================"
    echo "Smoke Test Summary"
    echo "============================================================"
    echo -e "Tests Passed: ${GREEN}${TESTS_PASSED}${NC}"
    echo -e "Tests Failed: ${RED}${TESTS_FAILED}${NC}"
    echo "============================================================"

    if [ "$TESTS_FAILED" -eq 0 ]; then
        echo -e "${GREEN}All smoke tests passed!${NC}"
        return 0
    else
        echo -e "${RED}Some tests failed. Please check the logs above.${NC}"
        return 1
    fi
}

# ============================================================
# Main
# ============================================================

main() {
    log_info "Streaming Lakehouse Lab - Smoke Test"
    echo ""

    # Check if Docker is running
    if ! docker info >/dev/null 2>&1; then
        log_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi

    # Run tests
    run_smoke_tests

    # Print summary
    print_summary
}

# Run main function
main "$@"

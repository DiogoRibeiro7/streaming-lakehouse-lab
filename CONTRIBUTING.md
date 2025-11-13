# Contributing to Streaming Lakehouse Lab

Thank you for considering contributing to this project! This document provides guidelines and instructions for contributing.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Coding Standards](#coding-standards)
- [Testing](#testing)
- [Pull Request Process](#pull-request-process)

## Code of Conduct

This project adheres to a code of professional conduct. By participating, you are expected to uphold this standard.

## Getting Started

### Prerequisites

- Docker & Docker Compose v2
- Make
- Java 21 (for local development)
- Python 3.12 + Poetry
- Git

### Initial Setup

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/streaming-lakehouse-lab.git
   cd streaming-lakehouse-lab
   ```

3. Set up the development environment:
   ```bash
   make setup
   ```

4. Install pre-commit hooks:
   ```bash
   make install-hooks
   ```

## Development Workflow

### 1. Create a Feature Branch

```bash
git checkout -b feat/your-feature-name
```

Branch naming conventions:
- `feat/` - New features
- `fix/` - Bug fixes
- `docs/` - Documentation changes
- `refactor/` - Code refactoring
- `test/` - Test additions or changes
- `chore/` - Maintenance tasks

### 2. Make Your Changes

Follow the [coding standards](#coding-standards) outlined below.

### 3. Run Tests

```bash
# Python tests
make test-python

# Java tests
make test-java

# Run linters
make lint-python
make type-check
```

### 4. Commit Your Changes

Use conventional commit messages:

```bash
git commit -m "feat: add new Iceberg table optimization"
git commit -m "fix: resolve Kafka connection timeout"
git commit -m "docs: update README with new examples"
```

Commit message format:
```
<type>(<scope>): <subject>

<body>

<footer>
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `style`: Code style changes
- `refactor`: Code refactoring
- `perf`: Performance improvements
- `test`: Test changes
- `build`: Build system changes
- `ci`: CI/CD changes
- `chore`: Maintenance

### 5. Push and Create Pull Request

```bash
git push origin feat/your-feature-name
```

Then create a pull request on GitHub.

## Coding Standards

### Python

- **Style Guide**: PEP 8, enforced by Ruff
- **Type Hints**: Required for all functions (mypy strict mode)
- **Docstrings**: Required for all public functions and classes (Google style)
- **Line Length**: 100 characters
- **Imports**: Sorted with isort (managed by Ruff)

Example:
```python
def process_event(event_id: str, timestamp: int) -> dict[str, Any]:
    """
    Process a streaming event and extract metadata.

    Args:
        event_id: Unique event identifier
        timestamp: Event timestamp in milliseconds

    Returns:
        Dictionary containing processed event data

    Raises:
        ValueError: If event_id is empty
    """
    if not event_id:
        raise ValueError("event_id cannot be empty")

    # Implementation here
    return {"id": event_id, "ts": timestamp}
```

### Java

- **Style Guide**: Google Java Style Guide
- **Documentation**: Javadoc for all public classes and methods
- **Type Safety**: Use generics, avoid raw types
- **Null Safety**: Use `@Nullable` annotations where appropriate

Example:
```java
/**
 * Processes streaming events and extracts metadata.
 *
 * @param eventId Unique event identifier
 * @param timestamp Event timestamp in milliseconds
 * @return Map containing processed event data
 * @throws IllegalArgumentException if eventId is null or empty
 */
public Map<String, Object> processEvent(String eventId, long timestamp) {
    if (eventId == null || eventId.isEmpty()) {
        throw new IllegalArgumentException("eventId cannot be null or empty");
    }

    // Implementation here
    return Map.of("id", eventId, "ts", timestamp);
}
```

### SQL

- **Keywords**: UPPERCASE
- **Identifiers**: lowercase_with_underscores
- **Indentation**: 2 spaces
- **Comments**: Use `--` for single-line, `/* */` for multi-line

### Docker/YAML

- **Indentation**: 2 spaces (never tabs)
- **Comments**: Use `#` for documentation
- **Service naming**: lowercase-with-hyphens

## Testing

### Test Coverage

- Aim for > 80% code coverage
- All new features must include tests
- All bug fixes must include regression tests

### Test Categories

**Unit Tests**: Fast, isolated tests
```bash
pytest tests/ -m "not integration"
```

**Integration Tests**: Tests with Docker containers
```bash
pytest tests/ -m integration
```

**Smoke Tests**: End-to-end validation
```bash
make test-smoke
```

### Writing Tests

- Use descriptive test names: `test_kafka_connection_retries_on_failure`
- Follow AAA pattern: Arrange, Act, Assert
- Mock external dependencies
- Clean up resources in teardown

## Pull Request Process

### Before Submitting

1. **Rebase on main**: Ensure your branch is up to date
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

2. **Run all checks**:
   ```bash
   make run-hooks
   make test-python
   make test-java
   ```

3. **Update documentation**: If you changed behavior or added features

### PR Description Template

```markdown
## Description
Brief description of what this PR does

## Motivation
Why is this change needed?

## Changes
- List of changes
- Use bullet points

## Testing
How was this tested?

## Checklist
- [ ] Tests added/updated
- [ ] Documentation updated
- [ ] Pre-commit hooks pass
- [ ] All tests pass
- [ ] Follows coding standards
```

### Review Process

1. At least one approval required
2. All CI checks must pass
3. No merge conflicts
4. Code owner review (for certain paths)

### After Merge

- Delete your feature branch
- Update your local main branch

## Project Structure

```
streaming-lakehouse-lab/
├── docker/                 # Docker Compose configurations
├── flink-jobs/            # Flink streaming jobs
│   ├── python/           # PyFlink jobs
│   └── java/             # Java Flink jobs
├── sql/                   # SQL scripts
├── scripts/              # Utility scripts
├── tests/                # Test suite
└── .devcontainer/        # Dev container configuration
```

## Need Help?

- Check existing issues and pull requests
- Review the [README](README.md) for project overview
- Ask questions in issue discussions

## Recognition

Contributors are recognized in the project README. Thank you for your contributions!

<!-- MISSING_DOC: Add troubleshooting guide for common development issues -->
<!-- MISSING_VALIDATION: Add PR template automation -->

# ============================================================
# Dev Container Dockerfile
# ============================================================
# Multi-stage build for development environment with
# Python 3.12, Java 21, and development tools
# ============================================================

FROM mcr.microsoft.com/devcontainers/python:3.12

# Set labels
LABEL maintainer="streaming-lakehouse-lab"
LABEL description="Development container for Flink streaming lakehouse"

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VERSION=1.8.0 \
    JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# ------------------------------------------------------------
# Install system dependencies
# ------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Build tools
    build-essential \
    ca-certificates \
    curl \
    wget \
    git \
    vim \
    # Java
    openjdk-21-jdk \
    # PostgreSQL client
    postgresql-client \
    # Network tools
    netcat-openbsd \
    iputils-ping \
    dnsutils \
    # Process monitoring
    htop \
    # Shell
    zsh \
    && rm -rf /var/lib/apt/lists/*

# ------------------------------------------------------------
# Install Poetry
# ------------------------------------------------------------
RUN curl -sSL https://install.python-poetry.org | python3 - \
    && ln -s /root/.local/bin/poetry /usr/local/bin/poetry

# Configure Poetry
RUN poetry config virtualenvs.create false

# ------------------------------------------------------------
# Install Gradle (comes with devcontainer feature)
# ------------------------------------------------------------
# Gradle will be installed via devcontainer feature

# ------------------------------------------------------------
# Install additional development tools
# ------------------------------------------------------------
RUN pip install --no-cache-dir \
    ipython \
    ipdb \
    rich

# ------------------------------------------------------------
# Create non-root user
# ------------------------------------------------------------
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

# The vscode user is already created by the base image,
# just ensure proper ownership
RUN mkdir -p /workspace && chown -R ${USERNAME}:${USERNAME} /workspace

# ------------------------------------------------------------
# Set working directory
# ------------------------------------------------------------
WORKDIR /workspace

# Switch to non-root user
USER ${USERNAME}

# Install oh-my-zsh for better shell experience
RUN sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" "" --unattended

# Set default shell
SHELL ["/bin/zsh", "-c"]

# MISSING_VALIDATION: Add security scanning for container image
# MISSING_DOC: Add custom development tooling documentation

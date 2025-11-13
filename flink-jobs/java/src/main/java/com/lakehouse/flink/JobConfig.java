package com.lakehouse.flink;

import java.util.Objects;

/**
 * Configuration class for Flink streaming jobs.
 *
 * <p>Loads configuration from environment variables with sensible defaults.
 * This class follows the immutable builder pattern for thread-safety.
 *
 * <p>Environment Variables:
 * <ul>
 *   <li>KAFKA_BOOTSTRAP_SERVERS: Kafka broker addresses</li>
 *   <li>KAFKA_TOPIC_EVENTS: Input Kafka topic name</li>
 *   <li>ICEBERG_CATALOG_NAME: Iceberg catalog name</li>
 *   <li>ICEBERG_WAREHOUSE: S3/MinIO warehouse path</li>
 *   <li>ICEBERG_NAMESPACE: Database/namespace in catalog</li>
 *   <li>CATALOG_JDBC_URL: PostgreSQL JDBC URL for catalog</li>
 *   <li>CATALOG_JDBC_USER: PostgreSQL username</li>
 *   <li>CATALOG_JDBC_PASSWORD: PostgreSQL password</li>
 *   <li>AWS_S3_ENDPOINT: MinIO/S3 endpoint URL</li>
 *   <li>AWS_ACCESS_KEY_ID: S3 access key</li>
 *   <li>AWS_SECRET_ACCESS_KEY: S3 secret key</li>
 *   <li>FLINK_PARALLELISM: Job parallelism (default: 2)</li>
 *   <li>FLINK_CHECKPOINT_INTERVAL: Checkpoint interval in ms (default: 60000)</li>
 * </ul>
 *
 * @author Streaming Lakehouse Lab
 * @version 0.1.0
 */
public class JobConfig {

    // Kafka configuration
    private final String kafkaBootstrapServers;
    private final String kafkaTopicEvents;
    private final String kafkaGroupId;

    // Iceberg configuration
    private final String icebergCatalogName;
    private final String icebergWarehouse;
    private final String icebergNamespace;

    // PostgreSQL catalog configuration
    private final String catalogJdbcUrl;
    private final String catalogJdbcUser;
    private final String catalogJdbcPassword;

    // S3/MinIO configuration
    private final String s3Endpoint;
    private final String s3AccessKey;
    private final String s3SecretKey;

    // Flink configuration
    private final int parallelism;
    private final long checkpointInterval;

    /**
     * Private constructor. Use {@link #fromEnvironment()} to create instances.
     */
    private JobConfig(Builder builder) {
        this.kafkaBootstrapServers = builder.kafkaBootstrapServers;
        this.kafkaTopicEvents = builder.kafkaTopicEvents;
        this.kafkaGroupId = builder.kafkaGroupId;
        this.icebergCatalogName = builder.icebergCatalogName;
        this.icebergWarehouse = builder.icebergWarehouse;
        this.icebergNamespace = builder.icebergNamespace;
        this.catalogJdbcUrl = builder.catalogJdbcUrl;
        this.catalogJdbcUser = builder.catalogJdbcUser;
        this.catalogJdbcPassword = builder.catalogJdbcPassword;
        this.s3Endpoint = builder.s3Endpoint;
        this.s3AccessKey = builder.s3AccessKey;
        this.s3SecretKey = builder.s3SecretKey;
        this.parallelism = builder.parallelism;
        this.checkpointInterval = builder.checkpointInterval;
    }

    /**
     * Creates a JobConfig instance from environment variables.
     *
     * @return JobConfig with values loaded from environment
     */
    public static JobConfig fromEnvironment() {
        return new Builder()
                .kafkaBootstrapServers(getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
                .kafkaTopicEvents(getEnv("KAFKA_TOPIC_EVENTS", "streaming.events"))
                .kafkaGroupId(getEnv("KAFKA_GROUP_ID", "flink-consumer"))
                .icebergCatalogName(getEnv("ICEBERG_CATALOG_NAME", "iceberg_catalog"))
                .icebergWarehouse(getEnv("ICEBERG_WAREHOUSE", "s3a://lakehouse/warehouse"))
                .icebergNamespace(getEnv("ICEBERG_NAMESPACE", "streaming_lakehouse"))
                .catalogJdbcUrl(getEnv("CATALOG_JDBC_URL",
                        "jdbc:postgresql://postgres:5432/iceberg_catalog"))
                .catalogJdbcUser(getEnv("CATALOG_JDBC_USER", "iceberg"))
                .catalogJdbcPassword(getEnv("CATALOG_JDBC_PASSWORD", "iceberg123"))
                .s3Endpoint(getEnv("AWS_S3_ENDPOINT", "http://minio:9000"))
                .s3AccessKey(getEnv("AWS_ACCESS_KEY_ID", "admin"))
                .s3SecretKey(getEnv("AWS_SECRET_ACCESS_KEY", "password123"))
                .parallelism(Integer.parseInt(getEnv("FLINK_PARALLELISM", "2")))
                .checkpointInterval(Long.parseLong(getEnv("FLINK_CHECKPOINT_INTERVAL", "60000")))
                .build();
    }

    /**
     * Helper method to get environment variable with default value.
     *
     * @param key Environment variable name
     * @param defaultValue Default value if not set
     * @return Environment variable value or default
     */
    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }

    // Getters

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getKafkaTopicEvents() {
        return kafkaTopicEvents;
    }

    public String getKafkaGroupId() {
        return kafkaGroupId;
    }

    public String getIcebergCatalogName() {
        return icebergCatalogName;
    }

    public String getIcebergWarehouse() {
        return icebergWarehouse;
    }

    public String getIcebergNamespace() {
        return icebergNamespace;
    }

    public String getCatalogJdbcUrl() {
        return catalogJdbcUrl;
    }

    public String getCatalogJdbcUser() {
        return catalogJdbcUser;
    }

    public String getCatalogJdbcPassword() {
        return catalogJdbcPassword;
    }

    public String getS3Endpoint() {
        return s3Endpoint;
    }

    public String getS3AccessKey() {
        return s3AccessKey;
    }

    public String getS3SecretKey() {
        return s3SecretKey;
    }

    public int getParallelism() {
        return parallelism;
    }

    public long getCheckpointInterval() {
        return checkpointInterval;
    }

    @Override
    public String toString() {
        return "JobConfig{" +
                "kafkaBootstrapServers='" + kafkaBootstrapServers + '\'' +
                ", kafkaTopicEvents='" + kafkaTopicEvents + '\'' +
                ", kafkaGroupId='" + kafkaGroupId + '\'' +
                ", icebergCatalogName='" + icebergCatalogName + '\'' +
                ", icebergWarehouse='" + icebergWarehouse + '\'' +
                ", icebergNamespace='" + icebergNamespace + '\'' +
                ", catalogJdbcUrl='" + catalogJdbcUrl + '\'' +
                ", catalogJdbcUser='" + catalogJdbcUser + '\'' +
                ", s3Endpoint='" + s3Endpoint + '\'' +
                ", parallelism=" + parallelism +
                ", checkpointInterval=" + checkpointInterval +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobConfig jobConfig = (JobConfig) o;
        return parallelism == jobConfig.parallelism &&
                checkpointInterval == jobConfig.checkpointInterval &&
                Objects.equals(kafkaBootstrapServers, jobConfig.kafkaBootstrapServers) &&
                Objects.equals(kafkaTopicEvents, jobConfig.kafkaTopicEvents) &&
                Objects.equals(icebergCatalogName, jobConfig.icebergCatalogName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kafkaBootstrapServers, kafkaTopicEvents, icebergCatalogName,
                parallelism, checkpointInterval);
    }

    /**
     * Builder class for JobConfig.
     */
    public static class Builder {
        private String kafkaBootstrapServers;
        private String kafkaTopicEvents;
        private String kafkaGroupId;
        private String icebergCatalogName;
        private String icebergWarehouse;
        private String icebergNamespace;
        private String catalogJdbcUrl;
        private String catalogJdbcUser;
        private String catalogJdbcPassword;
        private String s3Endpoint;
        private String s3AccessKey;
        private String s3SecretKey;
        private int parallelism = 2;
        private long checkpointInterval = 60000L;

        public Builder kafkaBootstrapServers(String kafkaBootstrapServers) {
            this.kafkaBootstrapServers = kafkaBootstrapServers;
            return this;
        }

        public Builder kafkaTopicEvents(String kafkaTopicEvents) {
            this.kafkaTopicEvents = kafkaTopicEvents;
            return this;
        }

        public Builder kafkaGroupId(String kafkaGroupId) {
            this.kafkaGroupId = kafkaGroupId;
            return this;
        }

        public Builder icebergCatalogName(String icebergCatalogName) {
            this.icebergCatalogName = icebergCatalogName;
            return this;
        }

        public Builder icebergWarehouse(String icebergWarehouse) {
            this.icebergWarehouse = icebergWarehouse;
            return this;
        }

        public Builder icebergNamespace(String icebergNamespace) {
            this.icebergNamespace = icebergNamespace;
            return this;
        }

        public Builder catalogJdbcUrl(String catalogJdbcUrl) {
            this.catalogJdbcUrl = catalogJdbcUrl;
            return this;
        }

        public Builder catalogJdbcUser(String catalogJdbcUser) {
            this.catalogJdbcUser = catalogJdbcUser;
            return this;
        }

        public Builder catalogJdbcPassword(String catalogJdbcPassword) {
            this.catalogJdbcPassword = catalogJdbcPassword;
            return this;
        }

        public Builder s3Endpoint(String s3Endpoint) {
            this.s3Endpoint = s3Endpoint;
            return this;
        }

        public Builder s3AccessKey(String s3AccessKey) {
            this.s3AccessKey = s3AccessKey;
            return this;
        }

        public Builder s3SecretKey(String s3SecretKey) {
            this.s3SecretKey = s3SecretKey;
            return this;
        }

        public Builder parallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public Builder checkpointInterval(long checkpointInterval) {
            this.checkpointInterval = checkpointInterval;
            return this;
        }

        public JobConfig build() {
            return new JobConfig(this);
        }
    }
}

// MISSING_VALIDATION: Add configuration validation (non-null checks, ranges)
// MISSING_TEST: Add unit tests for configuration loading
// MISSING_DOC: Add configuration validation examples

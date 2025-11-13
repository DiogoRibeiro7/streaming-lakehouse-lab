package com.lakehouse.dedup;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;

/**
 * Stateful Event Deduplication Job with TTL.
 *
 * <p>This Flink streaming job demonstrates stateful deduplication using keyed ValueState
 * with Time-To-Live (TTL) configuration. It reads events from a Kafka source topic,
 * deduplicates based on event ID, and writes unique events to a Kafka sink topic.
 *
 * <p>The job uses event-time processing with watermarks to handle out-of-order events
 * and automatic state cleanup via TTL to prevent unbounded state growth.
 *
 * <h2>Architecture:</h2>
 * <pre>
 * [Kafka Source] → [Parse JSON] → [Key by ID] → [Dedup Filter] → [Kafka Sink]
 *                                     ↓
 *                             [ValueState + TTL]
 * </pre>
 *
 * <h2>Input Event Schema (JSON):</h2>
 * <pre>
 * {
 *   "event_id": "unique-event-123",
 *   "timestamp": 1699900000000,
 *   "data": "..."
 * }
 * </pre>
 *
 * <h2>Environment Variables:</h2>
 * <ul>
 *   <li><b>KAFKA_BOOTSTRAP_SERVERS</b>: Kafka broker addresses (default: localhost:9092)</li>
 *   <li><b>KAFKA_INPUT_TOPIC</b>: Source topic for events (default: events-raw)</li>
 *   <li><b>KAFKA_OUTPUT_TOPIC</b>: Sink topic for deduplicated events (default: events-deduped)</li>
 *   <li><b>KAFKA_GROUP_ID</b>: Consumer group ID (default: dedup-job-group)</li>
 *   <li><b>STATE_TTL_MINUTES</b>: State TTL in minutes (default: 10)</li>
 *   <li><b>FLINK_PARALLELISM</b>: Job parallelism (default: 2)</li>
 *   <li><b>FLINK_CHECKPOINT_INTERVAL</b>: Checkpoint interval in ms (default: 60000)</li>
 * </ul>
 *
 * <h2>Usage:</h2>
 * <pre>
 * // Local execution
 * ./gradlew :flink-java-jobs:run
 *
 * // Build shadow JAR
 * ./gradlew :flink-java-jobs:shadowJar
 *
 * // Submit to Flink cluster
 * flink run build/libs/flink-dedup-job-0.1.0.jar
 * </pre>
 *
 * @author Lakehouse Team
 * @version 0.1.0
 * @since 2025-01-13
 */
public class StatefulDedupJob {

    private static final Logger LOG = LoggerFactory.getLogger(StatefulDedupJob.class);

    // Configuration constants with sensible defaults
    private static final String DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_INPUT_TOPIC = "events-raw";
    private static final String DEFAULT_OUTPUT_TOPIC = "events-deduped";
    private static final String DEFAULT_GROUP_ID = "dedup-job-group";
    private static final int DEFAULT_STATE_TTL_MINUTES = 10;
    private static final int DEFAULT_PARALLELISM = 2;
    private static final long DEFAULT_CHECKPOINT_INTERVAL = 60000L;

    /**
     * Main entry point for the Stateful Deduplication Job.
     *
     * <p>Initializes the Flink execution environment, configures checkpointing,
     * sets up Kafka source and sink, and executes the deduplication pipeline.
     *
     * @param args Command line arguments (currently unused)
     * @throws Exception if job execution fails
     */
    public static void main(String[] args) throws Exception {
        // Load configuration from environment variables
        final JobConfig config = loadConfiguration();

        LOG.info("Starting Stateful Deduplication Job");
        LOG.info("Configuration: {}", config);

        // Initialize Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set parallelism
        env.setParallelism(config.parallelism);
        LOG.info("Parallelism set to: {}", config.parallelism);

        // Enable checkpointing for fault tolerance and exactly-once semantics
        env.enableCheckpointing(config.checkpointInterval);
        LOG.info("Checkpointing enabled with interval: {}ms", config.checkpointInterval);

        // Build and execute the deduplication pipeline
        buildPipeline(env, config);

        // Execute the job
        env.execute("Stateful Event Deduplication Job");
    }

    /**
     * Load job configuration from environment variables.
     *
     * <p>Reads configuration values from system environment variables with
     * fallback to default values if not specified.
     *
     * @return JobConfig instance with loaded configuration
     */
    private static JobConfig loadConfiguration() {
        return new JobConfig(
            getEnv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP_SERVERS),
            getEnv("KAFKA_INPUT_TOPIC", DEFAULT_INPUT_TOPIC),
            getEnv("KAFKA_OUTPUT_TOPIC", DEFAULT_OUTPUT_TOPIC),
            getEnv("KAFKA_GROUP_ID", DEFAULT_GROUP_ID),
            Integer.parseInt(getEnv("STATE_TTL_MINUTES", String.valueOf(DEFAULT_STATE_TTL_MINUTES))),
            Integer.parseInt(getEnv("FLINK_PARALLELISM", String.valueOf(DEFAULT_PARALLELISM))),
            Long.parseLong(getEnv("FLINK_CHECKPOINT_INTERVAL", String.valueOf(DEFAULT_CHECKPOINT_INTERVAL)))
        );
    }

    /**
     * Get environment variable with default fallback.
     *
     * @param key Environment variable name
     * @param defaultValue Default value if not set
     * @return Environment variable value or default
     */
    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }

    /**
     * Build the deduplication pipeline.
     *
     * <p>Pipeline stages:
     * <ol>
     *   <li>Read from Kafka source topic</li>
     *   <li>Assign watermarks for event-time processing</li>
     *   <li>Key by event_id field</li>
     *   <li>Apply stateful deduplication filter with TTL</li>
     *   <li>Write unique events to Kafka sink topic</li>
     * </ol>
     *
     * @param env Flink execution environment
     * @param config Job configuration
     */
    private static void buildPipeline(
            StreamExecutionEnvironment env,
            JobConfig config) {

        // Stage 1: Create Kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(config.kafkaBootstrapServers)
            .setTopics(config.inputTopic)
            .setGroupId(config.groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        LOG.info("Kafka source created: topic={}, brokers={}",
            config.inputTopic, config.kafkaBootstrapServers);

        // Stage 2: Read from Kafka and assign watermarks
        // Using bounded out-of-orderness watermark strategy with 5 second max delay
        DataStream<String> sourceStream = env
            .fromSource(
                kafkaSource,
                WatermarkStrategy
                    .<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((event, timestamp) -> extractTimestamp(event)),
                "Kafka Source"
            );

        // Stage 3: Key by event ID and apply deduplication
        DataStream<String> dedupedStream = sourceStream
            .keyBy(StatefulDedupJob::extractEventId)
            .filter(new DeduplicationFilter(config.stateTtlMinutes));

        // Stage 4: Create Kafka sink
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
            .setBootstrapServers(config.kafkaBootstrapServers)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(config.outputTopic)
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build()
            )
            .build();

        LOG.info("Kafka sink created: topic={}, brokers={}",
            config.outputTopic, config.kafkaBootstrapServers);

        // Stage 5: Write deduplicated events to Kafka
        dedupedStream.sinkTo(kafkaSink);

        LOG.info("Pipeline built successfully");
    }

    /**
     * Extract event ID from JSON event string.
     *
     * <p>Parses the JSON event and extracts the "event_id" field used as the
     * deduplication key. If parsing fails or field is missing, returns a
     * default value to prevent job failure.
     *
     * @param event JSON event string
     * @return Event ID or "unknown" if extraction fails
     */
    private static String extractEventId(String event) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(event);

            // Extract event_id field (primary key for deduplication)
            if (node.has("event_id")) {
                return node.get("event_id").asText();
            }

            LOG.warn("Event missing 'event_id' field: {}", event);
            return "unknown-" + event.hashCode();

        } catch (Exception e) {
            LOG.error("Failed to parse event: {}", event, e);
            return "error-" + event.hashCode();
        }
    }

    /**
     * Extract timestamp from JSON event for watermark generation.
     *
     * <p>Attempts to extract the "timestamp" field from the event. If not present
     * or parsing fails, falls back to current processing time.
     *
     * @param event JSON event string
     * @return Event timestamp in milliseconds (epoch)
     */
    private static long extractTimestamp(String event) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(event);

            // Extract timestamp field for event-time processing
            if (node.has("timestamp")) {
                return node.get("timestamp").asLong();
            }

            // Fallback to processing time if timestamp missing
            LOG.warn("Event missing 'timestamp' field, using processing time: {}", event);
            return System.currentTimeMillis();

        } catch (Exception e) {
            LOG.error("Failed to extract timestamp from event: {}", event, e);
            return System.currentTimeMillis();
        }
    }

    /**
     * Stateful deduplication filter function with TTL.
     *
     * <p>Uses Flink's keyed ValueState to track previously seen event IDs.
     * State is automatically cleaned up after the configured TTL period to
     * prevent unbounded state growth.
     *
     * <p>The filter:
     * <ul>
     *   <li>Returns true (keep) for first occurrence of an event ID</li>
     *   <li>Returns false (filter out) for duplicate event IDs within TTL window</li>
     *   <li>Automatically expires state entries after TTL period</li>
     * </ul>
     */
    public static class DeduplicationFilter extends RichFilterFunction<String> {

        private static final long serialVersionUID = 1L;
        private static final Logger LOG = LoggerFactory.getLogger(DeduplicationFilter.class);

        private final int stateTtlMinutes;

        // ValueState to track whether an event ID has been seen
        // Stores Boolean.TRUE if event has been processed, null otherwise
        private transient ValueState<Boolean> seenState;

        /**
         * Constructor for DeduplicationFilter.
         *
         * @param stateTtlMinutes State TTL in minutes (auto-cleanup period)
         */
        public DeduplicationFilter(int stateTtlMinutes) {
            this.stateTtlMinutes = stateTtlMinutes;
        }

        /**
         * Initialize the state when the function starts.
         *
         * <p>Configures ValueState with TTL settings:
         * <ul>
         *   <li>TTL duration: Configurable minutes (default 10)</li>
         *   <li>Update type: OnCreateAndWrite (reset TTL on every update)</li>
         *   <li>State visibility: NeverReturnExpired (don't return expired state)</li>
         *   <li>Cleanup strategy: Incremental cleanup in background</li>
         * </ul>
         *
         * @param parameters Configuration parameters from Flink runtime
         * @throws Exception if state initialization fails
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            // Configure State TTL for automatic cleanup
            StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.minutes(stateTtlMinutes))
                // Update TTL on create and write operations
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                // Never return expired state values
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                // Enable incremental cleanup in background
                .cleanupIncrementally(1000, true)
                .build();

            // Create ValueState descriptor with TTL configuration
            ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>(
                "seen-events",     // State name
                Boolean.class      // State type
            );
            descriptor.enableTimeToLive(ttlConfig);

            // Initialize state from runtime context
            seenState = getRuntimeContext().getState(descriptor);

            LOG.info("Deduplication filter initialized with TTL: {} minutes", stateTtlMinutes);
        }

        /**
         * Filter function to deduplicate events.
         *
         * <p>For each event (keyed by event_id):
         * <ol>
         *   <li>Check if event ID exists in state</li>
         *   <li>If null (first occurrence): Update state and return true (keep event)</li>
         *   <li>If not null (duplicate): Return false (filter out event)</li>
         * </ol>
         *
         * <p>The state is automatically cleaned up after TTL expires, allowing
         * the same event ID to be processed again after the TTL window.
         *
         * @param event The input event (JSON string)
         * @return true if event should be kept (first occurrence), false if duplicate
         * @throws Exception if state access fails
         */
        @Override
        public boolean filter(String event) throws Exception {
            // Check if this event ID has been seen within the TTL window
            Boolean hasBeenSeen = seenState.value();

            if (hasBeenSeen == null) {
                // First occurrence of this event ID (or state has expired)
                // Mark as seen and allow the event through
                seenState.update(Boolean.TRUE);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("First occurrence - keeping event: {}", extractEventId(event));
                }

                return true; // Keep the event

            } else {
                // Duplicate event within TTL window - filter it out
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Duplicate detected - filtering event: {}", extractEventId(event));
                }

                return false; // Filter out the duplicate
            }
        }
    }

    /**
     * Job configuration container.
     *
     * <p>Encapsulates all configuration parameters for the deduplication job.
     */
    private static class JobConfig {
        final String kafkaBootstrapServers;
        final String inputTopic;
        final String outputTopic;
        final String groupId;
        final int stateTtlMinutes;
        final int parallelism;
        final long checkpointInterval;

        JobConfig(
                String kafkaBootstrapServers,
                String inputTopic,
                String outputTopic,
                String groupId,
                int stateTtlMinutes,
                int parallelism,
                long checkpointInterval) {
            this.kafkaBootstrapServers = kafkaBootstrapServers;
            this.inputTopic = inputTopic;
            this.outputTopic = outputTopic;
            this.groupId = groupId;
            this.stateTtlMinutes = stateTtlMinutes;
            this.parallelism = parallelism;
            this.checkpointInterval = checkpointInterval;
        }

        @Override
        public String toString() {
            return String.format(
                "JobConfig{brokers=%s, input=%s, output=%s, groupId=%s, ttl=%dmin, parallelism=%d, checkpoint=%dms}",
                kafkaBootstrapServers, inputTopic, outputTopic, groupId,
                stateTtlMinutes, parallelism, checkpointInterval
            );
        }
    }
}

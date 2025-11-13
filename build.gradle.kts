plugins {
    java
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "com.lakehouse"
version = "0.1.0"

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

repositories {
    mavenCentral()
    maven {
        url = uri("https://repository.apache.org/content/repositories/snapshots/")
    }
}

// Define versions for consistency
val flinkVersion = "2.1.0"
val icebergVersion = "1.10.0"
val kafkaVersion = "4.1.0"
val slf4jVersion = "2.0.9"
val junitVersion = "5.10.1"

dependencies {
    // Flink core dependencies
    implementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    implementation("org.apache.flink:flink-clients:$flinkVersion")
    implementation("org.apache.flink:flink-connector-base:$flinkVersion")

    // Kafka connector
    implementation("org.apache.flink:flink-connector-kafka:$flinkVersion")
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")

    // Iceberg connector
    implementation("org.apache.iceberg:iceberg-flink-runtime-2.1:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-core:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-api:$icebergVersion")

    // AWS SDK for MinIO/S3 access
    implementation("org.apache.iceberg:iceberg-aws:$icebergVersion")
    implementation("software.amazon.awssdk:s3:2.25.11")
    implementation("software.amazon.awssdk:sts:2.25.11")
    implementation("software.amazon.awssdk:glue:2.25.11")

    // PostgreSQL JDBC for catalog
    implementation("org.postgresql:postgresql:42.7.3")

    // Logging
    implementation("org.slf4j:slf4j-api:$slf4jVersion")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:2.21.1")
    runtimeOnly("org.apache.logging.log4j:log4j-api:2.21.1")
    runtimeOnly("org.apache.logging.log4j:log4j-core:2.21.1")

    // JSON processing
    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.1")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.16.1")

    // Testing
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-params:$junitVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    testImplementation("org.apache.flink:flink-test-utils:$flinkVersion")
    testImplementation("org.apache.flink:flink-runtime:$flinkVersion:tests")
    testImplementation("org.apache.flink:flink-streaming-java:$flinkVersion:tests")
    testImplementation("org.assertj:assertj-core:3.24.2")
    testImplementation("org.testcontainers:testcontainers:1.19.3")
    testImplementation("org.testcontainers:kafka:1.19.3")
    testImplementation("org.testcontainers:postgresql:1.19.3")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.addAll(
        listOf(
            "-Xlint:deprecation",
            "-Xlint:unchecked",
            "-parameters" // Preserve parameter names for reflection
        )
    )
}

tasks.test {
    useJUnitPlatform()

    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = false
        showExceptions = true
        showCauses = true
        showStackTraces = true
    }

    // Increase memory for tests
    maxHeapSize = "1g"
}

// Configure Shadow JAR for Flink job submission
tasks.shadowJar {
    archiveBaseName.set("flink-jobs")
    archiveClassifier.set("")
    archiveVersion.set(project.version.toString())

    // Include dependencies but exclude Flink and Hadoop (provided by cluster)
    dependencies {
        exclude(dependency("org.apache.flink:.*"))
        exclude(dependency("org.apache.hadoop:.*"))
        exclude(dependency("org.slf4j:.*"))
        exclude(dependency("org.apache.logging.log4j:.*"))
    }

    // Relocate dependencies to avoid conflicts
    relocate("com.google", "shadow.com.google")
    relocate("com.fasterxml.jackson", "shadow.com.fasterxml.jackson")

    mergeServiceFiles()
}

application {
    mainClass.set("com.lakehouse.flink.KafkaToIcebergJob")
}

// Custom tasks for running specific jobs
tasks.register<JavaExec>("runKafkaToIcebergJob") {
    group = "application"
    description = "Run Kafka to Iceberg streaming job"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.lakehouse.flink.KafkaToIcebergJob")
}

tasks.register<JavaExec>("runWindowAggregationJob") {
    group = "application"
    description = "Run windowed aggregation streaming job"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.lakehouse.flink.WindowAggregationJob")
}

// Code quality checks
tasks.register("checkFormat") {
    group = "verification"
    description = "Check Java code formatting"
    doLast {
        println("Code formatting check - MISSING_VALIDATION: Add Spotless or Checkstyle")
    }
}

// MISSING_VALIDATION: Add Spotless plugin for code formatting
// MISSING_VALIDATION: Add SpotBugs or ErrorProne for static analysis
// MISSING_TEST: Add integration tests with Testcontainers

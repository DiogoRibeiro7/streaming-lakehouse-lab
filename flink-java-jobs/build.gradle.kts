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
// Note: Using Flink 1.19.1 as Flink 2.1.0 artifacts are not yet in Maven Central
val flinkVersion = "1.19.1"
val kafkaVersion = "3.5.1"
val slf4jVersion = "2.0.9"
val junitVersion = "5.10.1"

dependencies {
    // Flink core dependencies
    implementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    implementation("org.apache.flink:flink-clients:$flinkVersion")
    implementation("org.apache.flink:flink-connector-base:$flinkVersion")

    // Kafka connector
    implementation("org.apache.flink:flink-connector-kafka:3.1.0-1.18")
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")

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
    archiveBaseName.set("flink-dedup-job")
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
    mainClass.set("com.lakehouse.dedup.StatefulDedupJob")
}

// Custom task for running the dedup job
tasks.register<JavaExec>("runDedupJob") {
    group = "application"
    description = "Run stateful deduplication streaming job"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.lakehouse.dedup.StatefulDedupJob")
}

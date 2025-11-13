rootProject.name = "streaming-lakehouse-lab"

// Include subprojects
include("flink-java-jobs")

// Enable Gradle build cache
buildCache {
    local {
        isEnabled = true
        directory = File(rootDir, ".gradle/build-cache")
        removeUnusedEntriesAfterDays = 30
    }
}

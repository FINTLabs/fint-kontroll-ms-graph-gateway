plugins {
    id("org.springframework.boot") version "3.5.16"
    id("io.spring.dependency-management") version "1.1.7"
    id("org.jlleitschuh.gradle.ktlint") version "14.2.0"

    kotlin("jvm") version "2.3.10"
    kotlin("plugin.spring") version "2.4.20"
    kotlin("plugin.jpa") version "2.4.20"
}

group = "no.novari"
version = "0.0.1-SNAPSHOT"

kotlin {
    jvmToolchain(25)
}

repositories {
    mavenLocal()
    maven("https://repo.fintlabs.no/releases")
    mavenCentral()
}

dependencies {
    // Spring Boot
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.apache.tomcat.embed:tomcat-embed-core:10.1.59")
    implementation("io.netty:netty-handler:4.2.18.Final")
    implementation("org.springframework:spring-context-support")
    implementation("no.fintlabs:fint-kontroll-auth:1.3.8")
    runtimeOnly("io.micrometer:micrometer-registry-prometheus")
    implementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")
    implementation("org.apache.httpcomponents.core5:httpcore5-h2:5.4.3")
    implementation("org.apache.commons:commons-lang3:3.20.0")
    implementation("org.apache.commons:commons-compress:1.28.0")
    implementation("org.apache.logging.log4j:log4j-api:2.26.1")
    implementation("tools.jackson.core:jackson-databind:3.2.2")
    implementation("io.opentelemetry:opentelemetry-api:1.65.0")

    // Kotlin
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8")

    // Database (Postgres)
    implementation("org.postgresql:postgresql")
    implementation("org.flywaydb:flyway-core")
    implementation("org.flywaydb:flyway-database-postgresql")

    // Kafka
    implementation("org.springframework.kafka:spring-kafka")
    implementation("no.novari:kafka:6.0.0")
    implementation("at.yawk.lz4:lz4-java:1.11.2")

    // Microsoft Graph SDK (Java)
    implementation("com.microsoft.graph:microsoft-graph:6.65.0")
    implementation("com.azure:azure-identity:1.18.6")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test")
    testImplementation("io.mockk:mockk:1.13.10")
    testImplementation("org.jetbrains.kotlin:kotlin-reflect")
    testImplementation("org.testcontainers:junit-jupiter:1.19.8")
    testImplementation("org.testcontainers:kafka:1.19.8")
    testImplementation("org.testcontainers:postgresql:1.19.8")
    testImplementation("org.wiremock:wiremock:3.13.2")
}

tasks.named<Test>("test") {
    useJUnitPlatform {
        excludeTags("manual")
    }
    jvmArgs("-XX:+EnableDynamicAgentLoading", "--sun-misc-unsafe-memory-access=allow")
}
tasks.register<Test>("integrationTests") {
    description = "Runs tests tagged as manual"
    group = "verification"
    maxHeapSize = "2g"
    val testSourceSet = sourceSets.test.get()

    useJUnitPlatform {
        includeTags("manual")
    }

    testClassesDirs = testSourceSet.output.classesDirs
    classpath = testSourceSet.runtimeClasspath
}

ktlint {
    version.set("1.8.0")
    verbose.set(true)
    outputToConsole.set(true)
    reporters {
        reporter(org.jlleitschuh.gradle.ktlint.reporter.ReporterType.HTML)
    }
}

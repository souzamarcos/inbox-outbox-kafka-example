plugins {
    id("org.springframework.boot")
}

dependencies {
    implementation(project(":events"))

    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.kafka:spring-kafka")
    // No Spring Boot 4 a autoconfiguração do Kafka vive no módulo dedicado spring-boot-kafka.
    implementation("org.springframework.boot:spring-boot-kafka")

    runtimeOnly("org.postgresql:postgresql")
    // No Spring Boot 4 a autoconfiguração do Flyway vive no módulo dedicado spring-boot-flyway
    // (traz flyway-core transitivamente). Só flyway-core não ativa as migrações.
    implementation("org.springframework.boot:spring-boot-flyway")
    runtimeOnly("org.flywaydb:flyway-database-postgresql")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("org.testcontainers:testcontainers-postgresql")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

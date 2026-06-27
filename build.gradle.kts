// Build raiz do projeto multi-módulo.
// A raiz NÃO é uma aplicação: ela apenas declara os plugins (apply false) e
// centraliza a configuração comum dos submódulos (:events, :order-service, :notification-service).

plugins {
    java
    id("org.springframework.boot") version "4.1.0" apply false
    id("io.spring.dependency-management") version "1.1.7" apply false
}

allprojects {
    group = "com.marcos"
    version = "0.0.1-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java")
    apply(plugin = "io.spring.dependency-management")

    configure<JavaPluginExtension> {
        toolchain {
            languageVersion = JavaLanguageVersion.of(25)
        }
    }

    // Importa o BOM do Spring Boot em todos os módulos para alinhar versões
    // (spring-kafka, jackson, etc.) sem precisar fixar números manualmente.
    the<io.spring.gradle.dependencymanagement.dsl.DependencyManagementExtension>().apply {
        imports {
            mavenBom("org.springframework.boot:spring-boot-dependencies:4.1.0")
        }
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }
}

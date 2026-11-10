plugins {
    `java-library`
    id("jacoco")
    id("com.diffplug.spotless") version "8.10.4"
    id("com.vanniktech.maven.publish") version "0.37.0"
}

group = "io.github.openlogiclab"
version = System.getenv("GITHUB_REF_NAME")?.removePrefix("v") ?: "0.1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    api("org.apache.kafka:kafka-clients:4.4.0")

    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

tasks.compileJava {
    doFirst {
        options.compilerArgs.addAll(listOf(
            "--module-path", classpath.asPath
        ))
        classpath = files()
    }
}

tasks.javadoc {
    doFirst {
        val mp = classpath.asPath
        (options as CoreJavadocOptions).apply {
            addMultilineStringsOption("-module-path").value = listOf(mp)
        }
        classpath = files()
    }
}

spotless {
    java {
        licenseHeaderFile("gradle/license-header.txt")
        importOrder()
        removeUnusedImports()
        googleJavaFormat()
        targetExclude("build/**")
    }
}

mavenPublishing {
    publishToMavenCentral(automaticRelease = true)
    signAllPublications()

    pom {
        name.set("kafka-pipeline")
        description.set("Lightweight Kafka consumer pipeline with concurrent processing, backpressure, and sliding-window offset management")
        inceptionYear.set("2026")
        url.set("https://github.com/openlogiclab/kafka-pipeline")
        licenses {
            license {
                name.set("Apache License 2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                distribution.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
            }
        }
        developers {
            developer {
                id.set("zachhuan")
                url.set("https://github.com/zachhuan")
            }
        }
        scm {
            url.set("https://github.com/openlogiclab/kafka-pipeline")
            connection.set("scm:git:git://github.com/openlogiclab/kafka-pipeline.git")
            developerConnection.set("scm:git:ssh://git@github.com/openlogiclab/kafka-pipeline.git")
        }
    }
}

tasks.test {
    useJUnitPlatform()
    finalizedBy(tasks.jacocoTestReport)
}

// Tests against a real Kafka broker in Docker (Testcontainers). Kept out of the fast unit-test task;
// `check` runs them, so CI does too. Run alone with `./gradlew integrationTest`.
val integrationTestSourceSet: SourceSet = sourceSets.create("integrationTest") {
    compileClasspath += sourceSets.main.get().output
    runtimeClasspath += sourceSets.main.get().output
}

configurations[integrationTestSourceSet.implementationConfigurationName]
    .extendsFrom(configurations.testImplementation.get())
configurations[integrationTestSourceSet.runtimeOnlyConfigurationName]
    .extendsFrom(configurations.testRuntimeOnly.get())

dependencies {
    "integrationTestImplementation"("org.testcontainers:testcontainers-kafka:2.0.4")
    "integrationTestImplementation"("org.testcontainers:testcontainers-junit-jupiter:2.0.4")
    "integrationTestRuntimeOnly"("org.slf4j:slf4j-simple:2.0.20")
}

val integrationTest = tasks.register<Test>("integrationTest") {
    description = "Runs tests against a real Kafka broker (requires Docker)."
    group = "verification"
    testClassesDirs = integrationTestSourceSet.output.classesDirs
    classpath = integrationTestSourceSet.runtimeClasspath
    useJUnitPlatform()
    shouldRunAfter(tasks.test)
}

tasks.check {
    dependsOn(integrationTest)
}

tasks.jacocoTestReport {
    dependsOn(tasks.test)
    reports {
        xml.required = true
        html.required = true
        csv.required = false
    }
}

tasks.jacocoTestCoverageVerification {
    violationRules {
        rule {
            limit {
                minimum = "0.70".toBigDecimal()
            }
        }
    }
}

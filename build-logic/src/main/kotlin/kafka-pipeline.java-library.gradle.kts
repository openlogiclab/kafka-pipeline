// Conventions shared by every published module: Java 21 on the module path, formatting and license
// headers, unit and broker tests, coverage, and Maven Central publishing. A module's own build file
// only adds its dependencies and its artifact name.

plugins {
    `java-library`
    jacoco
    id("com.diffplug.spotless")
    id("com.vanniktech.maven.publish")
}

group = "io.github.openlogiclab"
// Releases are cut from a tag such as v0.4.0; anything else is a snapshot.
// A release is built from its tag (v0.4.0 -> 0.4.0); branch and pull-request builds are snapshots.
version =
    if (System.getenv("GITHUB_REF_TYPE") == "tag") {
        System.getenv("GITHUB_REF_NAME").removePrefix("v")
    } else {
        "0.4.0-SNAPSHOT"
    }

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

// Compile and document main code as a JPMS module, so module-info.java is enforced.
tasks.compileJava {
    doFirst {
        options.compilerArgs.addAll(listOf("--module-path", classpath.asPath))
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

// Everything except main (tests, integration tests) compiles and runs on the class
// path. Tests live in the packages they test, which a module path would treat as split packages.
tasks.withType<JavaCompile>().configureEach {
    if (name != "compileJava") modularity.inferModulePath = false
}
tasks.withType<Test>().configureEach {
    modularity.inferModulePath = false
}

spotless {
    java {
        licenseHeaderFile(rootProject.file("gradle/license-header.txt"))
        importOrder()
        removeUnusedImports()
        googleJavaFormat()
        targetExclude("build/**")
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
    "integrationTestRuntimeOnly"("org.slf4j:slf4j-simple:2.0.17")
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

mavenPublishing {
    publishToMavenCentral(automaticRelease = true)
    signAllPublications()

    pom {
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
